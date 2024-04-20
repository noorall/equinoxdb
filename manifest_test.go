package equinox

import (
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getTestChanges(creations, deletions int) manifestChanges {
	AssertTrue(creations >= deletions)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	changes := []*manifestChange{}
	tables := r.Perm(creations)
	pairs := [][]uint64{}
	// add a table into a random level
	for i := 0; i < creations; i++ {
		tid := uint64(tables[i])
		lid := uint64(r.Intn(10))
		changes = append(changes,
			newManifestChange(mcreate, tid, lid, ZSTDCompression))
		pairs = append(pairs, []uint64{tid, lid})
	}

	// delete a random table from a random level
	del := r.Perm(creations)
	for i := 0; i < deletions; i++ {
		pair := pairs[del[i]]
		changes = append(changes,
			newManifestChange(mdelete, pair[0], pair[1], ZSTDCompression))
	}

	return manifestChanges(changes)
}

func TestManifestApply(t *testing.T) {
	changes := []*manifestChange{
		newManifestChange(mcreate, 1, 0, ZSTDCompression),
		newManifestChange(mcreate, 2, 0, ZSTDCompression),
		newManifestChange(mcreate, 3, 1, ZSTDCompression),
		newManifestChange(mdelete, 2, 0, ZSTDCompression),
		newManifestChange(mdelete, 1, 0, ZSTDCompression),
		newManifestChange(mcreate, 1, 1, ZSTDCompression),
		newManifestChange(mcreate, 2, 1, ZSTDCompression),
	}

	expectedManifest := NewManifest()
	expectedManifest.creations = 5
	expectedManifest.deletions = 2
	expectedManifest.Levels = []levelsManifest{
		{
			Tables: map[uint64]struct{}{},
		},
		{
			Tables: map[uint64]struct{}{
				1: {},
				2: {},
				3: {},
			},
		},
	}
	expectedManifest.Tables = map[uint64]tableManifest{
		1: {level: 1, compression: ZSTDCompression},
		2: {level: 1, compression: ZSTDCompression},
		3: {level: 1, compression: ZSTDCompression},
	}

	actualManifest := NewManifest()
	require.NoError(t, applyManifestChanges(&actualManifest, changes))
	require.EqualValues(t, expectedManifest, actualManifest)
}

func TestManifestApplyDuplicateTables(t *testing.T) {
	changes := []*manifestChange{
		newManifestChange(mcreate, 1, 0, ZSTDCompression),
		newManifestChange(mcreate, 2, 0, ZSTDCompression),
		newManifestChange(mcreate, 3, 1, ZSTDCompression),
		newManifestChange(mdelete, 2, 0, ZSTDCompression),
		newManifestChange(mdelete, 1, 0, ZSTDCompression),
		newManifestChange(mcreate, 1, 1, ZSTDCompression),
		newManifestChange(mcreate, 1, 1, ZSTDCompression),
	}

	manifest := NewManifest()
	err := applyManifestChanges(&manifest, changes)
	require.EqualError(t, err, "MANIFEST Invalid, table 1 exists.")
}

func TestManifestApplyDeleteSameTables(t *testing.T) {
	changes := []*manifestChange{
		newManifestChange(mcreate, 1, 0, ZSTDCompression),
		newManifestChange(mcreate, 2, 0, ZSTDCompression),
		newManifestChange(mcreate, 3, 1, ZSTDCompression),
		newManifestChange(mdelete, 1, 0, ZSTDCompression),
		newManifestChange(mdelete, 1, 0, ZSTDCompression),
	}

	manifest := NewManifest()
	err := applyManifestChanges(&manifest, changes)
	require.EqualError(t, err, "MANIFEST removes non-existing table 1.")

	changes = []*manifestChange{
		newManifestChange(mcreate, 1, 0, ZSTDCompression),
		newManifestChange(mcreate, 2, 0, ZSTDCompression),
		newManifestChange(mcreate, 3, 1, ZSTDCompression),
		newManifestChange(mdelete, 1, 0, ZSTDCompression),
		newManifestChange(mdelete, 4, 0, ZSTDCompression),
	}

	manifest = NewManifest()
	err = applyManifestChanges(&manifest, changes)
	require.EqualError(t, err, "MANIFEST removes non-existing table 4.")
}

func TestManifestApplyInvalidOp(t *testing.T) {
	changes := []*manifestChange{
		newManifestChange(mcreate, 1, 0, ZSTDCompression),
		newManifestChange(mcreate, 2, 0, ZSTDCompression),
		newManifestChange(2, 3, 1, ZSTDCompression),
		newManifestChange(mdelete, 1, 0, ZSTDCompression),
		newManifestChange(mdelete, 2, 0, ZSTDCompression),
	}

	manifest := NewManifest()
	err := applyManifestChanges(&manifest, changes)
	require.EqualError(t, err, "MANIFEST has invalid operation 2.")
}

func TestManifestApplyRandom(t *testing.T) {
	manifest := NewManifest()
	randChanges := getTestChanges(10, 4)
	t.Logf("the random changes: %+v", randChanges)
	err := applyManifestChanges(&manifest, randChanges)
	require.NoError(t, err)
}

func TestManifestClone(t *testing.T) {
	manifest := NewManifest()
	changes := getTestChanges(10, 2)
	expected := NewManifest()
	require.NoError(t, applyManifestChanges(&manifest, changes))
	require.NoError(t, applyManifestChanges(&expected, manifest.toChanges()))

	clone := manifest.clone()
	require.EqualValues(t, expected, clone)
}

func TestManifestOpenNew(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	manifestFile, manifest, err := OpenManifestFile(dir)
	require.NoError(t, err)
	require.NotNil(t, manifestFile)
	defer manifestFile.Close()

	expectedManifest := NewManifest()
	filename := filepath.Join(dir, ManifestFilename)
	require.EqualValues(t, expectedManifest, manifest)
	require.EqualValues(t, filename, manifestFile.fd.Name())
	require.EqualValues(t, expectedManifest, manifestFile.manifest)

	// check file size
	stat, err := manifestFile.fd.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(16), stat.Size())
}

func TestManifestOpenExists(t *testing.T) {
	// open a new file
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	manifestFile, manifest, err := OpenManifestFile(dir)
	require.NoError(t, err)
	require.NotNil(t, manifestFile)
	defer manifestFile.Close()

	changes := getTestChanges(10, 5)
	// write changes
	require.NoError(t, manifestFile.AddManifestChange(changes))
	// apply changes to manifest too
	require.NoError(t, applyManifestChanges(&manifest, changes))

	// check file size
	stat, err := manifestFile.fd.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(16+8+changeSize*15), stat.Size())

	// close file and read back
	require.NoError(t, manifestFile.Close())
	manifestFile, manifest2, err := OpenManifestFile(dir)
	require.NoError(t, err)
	require.NotNil(t, manifestFile)

	// comapre manifests
	require.EqualValues(t, manifest, manifest2)
}

func writeBadDataTest(t *testing.T, offset int64, errContent string) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	manifestFile, _, err := OpenManifestFile(dir)
	require.NoError(t, err)
	require.NotNil(t, manifestFile)
	fp := manifestFile.fd
	_, err = fp.WriteAt([]byte{'X'}, offset)
	require.NoError(t, err)
	require.NoError(t, manifestFile.Close())

	manifestFile, _, err = OpenManifestFile(dir)
	defer func() {
		if manifestFile != nil {
			manifestFile.Close()
		}
	}()
	require.Error(t, err)
	require.Contains(t, err.Error(), errContent)
}

func TestManifestMagic(t *testing.T) {
	writeBadDataTest(t, 0, "bad magic")
}

func TestManifestVersion(t *testing.T) {
	writeBadDataTest(t, 5, "unsupported version")
}

func TestManifestChecksum(t *testing.T) {
	writeBadDataTest(t, 13, "checksum mismatch")
}

func TestManifestDeletionRewrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	manifestFile, _, err := OpenManifestFile(dir)
	require.NoError(t, err)
	require.NotNil(t, manifestFile)
	defer manifestFile.Close()
	// deletions < manifestDeletionsRewriteThreshold
	for i := 0; i < 19; i++ {
		changes := getTestChanges(500, 500)
		manifestFile.AddManifestChange(changes)
	}

	stat, err := manifestFile.fd.Stat()
	require.NoError(t, err)
	before := stat.Size()

	// add last changes, make it rewrite
	manifestFile.AddManifestChange(getTestChanges(600, 550))
	stat, err = manifestFile.fd.Stat()
	require.NoError(t, err)
	require.Equal(t, 50, manifestFile.manifest.creations)
	require.Equal(t, 0, manifestFile.manifest.deletions)

	after := stat.Size()

	require.True(t, before > after)
	require.Equal(t, int64(16+changeSize*50), after)
}
