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

package separator

import "gonum.org/v1/gonum/stat/distuv"

type SeparateDecider struct {
	dist     distuv.Normal
	zetaStep int
	avgTd    float64
	avgTg    float64

	separateThreshold int
	separateFactor    float64
}

func NewSeparateDecider(mu, sigma, separateFactor float64, zetaStep, separateThreshold int) *SeparateDecider {
	return &SeparateDecider{
		dist:              distuv.Normal{Mu: mu, Sigma: sigma},
		zetaStep:          zetaStep,
		avgTd:             0,
		avgTg:             0,
		separateThreshold: separateThreshold,
		separateFactor:    separateFactor,
	}
}

func (d *SeparateDecider) NeedSeparate(n, k int, keySize int, valueSize int) bool {
	waNonS := d.computeWaNoSep(n, k, keySize, valueSize)
	waS := d.computeWaSep(n, k, keySize, valueSize)
	if d.separateFactor*waS < waNonS {
		return true
	} else {
		return false
	}
}

func (d *SeparateDecider) computeWaNoSep(n, k int, keySize int, valueSize int) float64 {
	zeta := EstimateZetaSampled(k, n, d.zetaStep, d.avgTg, d.avgTd, d.dist)
	wa := float64(1) + zeta/float64(n)
	return wa * float64(keySize+valueSize)
}

func (d *SeparateDecider) computeWaSep(n, k int, keySize int, valueSize int) float64 {
	zeta := EstimateZetaSampled(k, n, d.zetaStep, d.avgTg, d.avgTd, d.dist)
	waKey := float64(1) + zeta/float64(n)
	var waValue float64
	if float64(d.separateThreshold) < zeta {
		waValue = float64(1) + zeta/float64(n)
	} else {
		waValue = 0
	}
	return waKey*float64(keySize) + waValue*float64(valueSize)
}

func (d *SeparateDecider) UpdateAvgTd(t float64) {
	if t <= 0 {
		return
	}
	d.avgTd = (t + d.avgTg) / 2
}

func (d *SeparateDecider) UpdateAvgTg(t float64) {
	if t <= 0 {
		return
	}
	d.avgTg = (t + d.avgTd) / 2
}

// TODO: optimize this part
func EstimateZetaSampled(k, n, sampleN int, tg, td float64, dist distuv.Normal) float64 {
	if sampleN <= 0 || k <= 0 {
		return 0
	}
	step := k / sampleN
	if step == 0 {
		step = 1
	}

	sum := 0.0
	for i := 0; i < k; i += step {
		integrand := func(x float64) float64 {
			prod := 1.0
			for j := 1; j <= n; j++ {
				t := float64(i+j) * (tg + td)
				prod *= dist.CDF(t + x)
			}
			return dist.Prob(x) * prod
		}
		// Sigma with only 5 times the points, simplifying calculations
		integral := integrate(integrand, 0, dist.Mu+5*dist.Sigma, 1000)
		sum += 1.0 - integral
	}
	scale := float64(k) / float64((k+step-1)/step)
	return sum * scale
}

func integrate(f func(x float64) float64, a, b float64, steps int) float64 {
	h := (b - a) / float64(steps)
	result := 0.0
	for i := 0; i < steps; i++ {
		x := a + float64(i)*h
		result += f(x) * h
	}
	return result
}
