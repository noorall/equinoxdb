/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package store

import (
	"container/heap"
	mapset "github.com/deckarep/golang-set/v2"
	"go.uber.org/zap"
	"sync/atomic"
)

type garbageFile struct {
	file      *ValueFile
	keys      mapset.Set[uint64]
	validSize uint64
	totalSize uint64
}

func (v *VFileManager) getGarbageFiles() []*ValueFile {
	gf := v.collectGarbageFile()
	if len(gf) == 0 {
		return nil
	}
	wg := BuildWeightedGraph(gf)
	gfIds := mapset.NewSet[int]()
	for gfIds.Cardinality() < wg.V.Cardinality() && gfIds.Cardinality() < v.valueFileGCMaxFiles {
		greedyKComponent(wg, v.valueFileGCMaxFiles-gfIds.Cardinality(), gfIds)
	}
	var gcFiles []*ValueFile
	for _, idx := range gfIds.ToSlice() {
		gcFiles = append(gcFiles, gf[idx].file)
	}
	return gcFiles
}

func (v *VFileManager) collectGarbageFile() []*garbageFile {
	v.filesLock.RLock()
	defer v.filesLock.RUnlock()

	var garbage []*garbageFile
	v.discard.Iterate(func(fid, count uint64) {
		if uint32(fid) >= atomic.LoadUint32(&v.maxFid) {
			return
		}
		vf, ok := v.filesMap[uint32(fid)]
		if !ok {
			v.discard.Reset(uint32(fid))
		}
		f, err := vf.Fd.Stat()
		if err != nil {
			v.logger.Error("Unable to get stats for value file", zap.Uint64("fid", fid), zap.Error(err))
			return
		}
		if count > uint64(v.valueFileGCThresholdRadio*float64(f.Size())) {
			gf := &garbageFile{
				file:      vf,
				keys:      mapset.NewSet[uint64](),
				validSize: uint64(f.Size()) - count,
				totalSize: uint64(f.Size()),
			}
			it := &ValueFileIterator{f: vf, header: &VFileHeader{}}
			// update key hashes
			for it.Next() == nil {
				header := it.ReadHeader()
				gf.keys.Add(header.KeyHash)
			}
			garbage = append(garbage, gf)
		}
	})

	return garbage
}

type Edge struct {
	I, J int
	W    float64
}

type WeightedGraph struct {
	V mapset.Set[int]
	E []Edge
}

func BuildWeightedGraph(files []*garbageFile) *WeightedGraph {
	keyToFiles := make(map[uint64][]int)
	V := mapset.NewSet[int]()

	for i, f := range files {
		for k := range f.keys.Iter() {
			keyToFiles[k] = append(keyToFiles[k], i)
			V.Add(i)
		}
	}

	var E []Edge
	seen := make(map[[2]int]bool)
	for _, fileIndices := range keyToFiles {
		n := len(fileIndices)
		for i := 0; i < n; i++ {
			for j := i + 1; j < n; j++ {
				fi, fj := fileIndices[i], fileIndices[j]
				if fi > fj {
					fi, fj = fj, fi
				}
				if seen[[2]int{fi, fj}] {
					continue
				}
				seen[[2]int{fi, fj}] = true
				ki, kj := files[fi].keys, files[fj].keys
				inter := ki.Intersect(kj)
				numIntersect := inter.Cardinality()
				total := ki.Union(kj).Cardinality()
				if total == 0 {
					continue
				}
				rij := (float64(files[fi].validSize) + float64(files[fj].validSize)) /
					(float64(files[fi].totalSize) + float64(files[fj].totalSize))
				w := (1 - rij + float64(numIntersect)/float64(total)) / (1 + rij - float64(1)/float64(total))
				E = append(E, Edge{I: fi, J: fj, W: w})
			}
		}
	}

	return &WeightedGraph{
		V: V,
		E: E,
	}
}

func greedyKComponent(graph *WeightedGraph, k int, cVertices mapset.Set[int]) mapset.Set[int] {
	maxEdge, ok := getMaxWeightEdge(graph, cVertices)
	if !ok {
		return nil
	}

	cEdges := make([]Edge, 0)
	maxHeap := &EdgeMaxHeap{maxEdge}
	heap.Init(maxHeap)

	for _, edge := range getConnectedEdges(maxEdge, graph, cVertices) {
		heap.Push(maxHeap, edge)
	}

	for maxHeap.Len() > 0 && cVertices.Cardinality() < k {
		edge := heap.Pop(maxHeap).(Edge)
		i, j := edge.I, edge.J
		if cVertices.Contains(i) && cVertices.Contains(j) {
			continue
		}
		cEdges = append(cEdges, edge)
		cVertices.Add(i)
		cVertices.Add(j)
		for _, next := range getConnectedEdges(edge, graph, cVertices) {
			heap.Push(maxHeap, next)
		}
	}
	return cVertices
}

func getMaxWeightEdge(graph *WeightedGraph, p mapset.Set[int]) (Edge, bool) {
	var maxEdge Edge
	maxWeight := -1.0
	found := false
	for _, e := range graph.E {
		if p.Contains(e.I) && p.Contains(e.J) {
			continue
		}
		if e.W > maxWeight {
			maxWeight = e.W
			maxEdge = e
			found = true
		}
	}
	return maxEdge, found
}

func getConnectedEdges(edge Edge, graph *WeightedGraph, p mapset.Set[int]) []Edge {
	res := make([]Edge, 0)
	for _, e := range graph.E {
		if p.Contains(e.I) && p.Contains(e.J) {
			continue
		}
		if e.I == edge.J || e.J == edge.I || e.I == edge.I || e.J == edge.J {
			res = append(res, e)
		}
	}
	return res
}

// EdgeMaxHeap is a max heap of Edges
type EdgeMaxHeap []Edge

// Len returns the number of elements in the heap
func (h EdgeMaxHeap) Len() int { return len(h) }

// Less compares two edges based on their weights
// Returns true if h[i] should be above h[j] in max heap
func (h EdgeMaxHeap) Less(i, j int) bool { return h[i].W > h[j].W }

// Swap exchanges two elements in the heap
func (h EdgeMaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an element to the heap
func (h *EdgeMaxHeap) Push(x interface{}) {
	*h = append(*h, x.(Edge))
}

// Pop removes and returns the top element
func (h *EdgeMaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
