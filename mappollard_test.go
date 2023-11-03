package utreexo

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"golang.org/x/exp/slices"
)

// Assert that MapPollard implements the UtreexoTest interface.
var _ UtreexoTest = (*MapPollard)(nil)

// cachedMapToString returns the cached map as a string.
//
// Implements the UtreexoTest interface.
func (p *MapPollard) cachedMapToString() string {
	return fmt.Sprintf("%v", p.CachedLeaves)
}

// nodeMapToString returns "n/a" as map pollard doesn't have a node map.
func (m *MapPollard) nodeMapToString() string {
	return "n/a"
}

// rootToString returns the roots as a string.
func (m *MapPollard) rootToString() string {
	return printHashes(m.GetRoots())
}

// sanityCheck checks that:
// 1: Unneeded nodes aren't cached.
// 2: Needed nodes for the cached leaves are cached.
// 3: Cached proof hashes up to the roots.
func (m *MapPollard) sanityCheck() error {
	err := m.checkCachedNodesAreRemembered()
	if err != nil {
		return err
	}

	err = m.checkProofNodes()
	if err != nil {
		return err
	}

	err = m.checkHashes()
	if err != nil {
		return err
	}

	return m.checkPruned()
}

// checkCachedNodesAreRemembered checks that cached leaves are present in m.Nodes and that they're
// marked to be remembered.
func (m *MapPollard) checkCachedNodesAreRemembered() error {
	for k, v := range m.CachedLeaves {
		leaf, found := m.Nodes[v]
		if !found {
			return fmt.Errorf("Cached node of %s at pos %d not cached in m.Nodes", k, v)
		}

		if leaf.Remember == false {
			return fmt.Errorf("Cached node of %s at pos %d not marked as remembered in m.Nodes", k, v)
		}
	}

	return nil
}

// checkPruned checks that unneeded nodes aren't cached.
func (m *MapPollard) checkPruned() error {
	neededPos := make(map[uint64]struct{})
	for _, v := range m.CachedLeaves {
		neededPos[v] = struct{}{}

		needs, computables := proofPositions([]uint64{v}, m.NumLeaves, m.TotalRows)
		for _, need := range needs {
			neededPos[need] = struct{}{}
		}

		for _, computable := range computables {
			neededPos[computable] = struct{}{}
		}
	}

	for _, pos := range RootPositions(m.NumLeaves, m.TotalRows) {
		neededPos[pos] = struct{}{}
	}

	for k, v := range m.Nodes {
		_, found := neededPos[k]
		if !found {
			return fmt.Errorf("Have node %s at pos %d in map "+
				"even though it's not needed.\nCachedLeaves:\n%v\nm.Nodes:\n%v\n",
				v, k, m.CachedLeaves, m.Nodes)
		}
	}

	return nil
}

// checkProofNodes checks that all the proof positions needed to cache a proof exists in the map
// of nodes.
func (m *MapPollard) checkProofNodes() error {
	// Sanity check.
	for k, v := range m.CachedLeaves {
		leaf, found := m.Nodes[v]
		if !found {
			return fmt.Errorf("Corrupted pollard. Missing cached leaf %s at %d", k, v)
		}

		if k != leaf.Hash {
			return fmt.Errorf("Corrupted pollard. Pos %d cached hash: %s, but have %s",
				v, k, leaf.Hash)
		}

		proofPos := proofPosition(v, m.NumLeaves, m.TotalRows)
		for _, pos := range proofPos {
			_, found := m.Nodes[pos]
			if !found {
				return fmt.Errorf("Corrupted pollard. Missing pos %d "+
					"needed for proving %d", pos, v)
			}
		}
	}

	return nil
}

// checkHashes checks that the leaves correctly hash up to the roots. Returns an error if
// any of the roots or the intermediate nodes don't match up with the calculated hashes.
func (m *MapPollard) checkHashes() error {
	if len(m.CachedLeaves) == 0 {
		return nil
	}

	leafHashes := make([]Hash, 0, len(m.CachedLeaves))
	for leafHash := range m.CachedLeaves {
		leafHashes = append(leafHashes, leafHash)
	}

	proof, err := m.Prove(leafHashes)
	if err != nil {
		return err
	}

	if m.TotalRows != treeRows(m.NumLeaves) {
		for i := range proof.Targets {
			proof.Targets[i] = translatePos(proof.Targets[i], m.TotalRows, treeRows(m.NumLeaves))
		}
	}

	haveRoots, rootPositions := m.getRoots()
	rootIndexes, err := Verify(Stump{Roots: haveRoots, NumLeaves: m.NumLeaves}, leafHashes, proof)
	if err != nil {
		return fmt.Errorf("Failed to verify proof:\n%s\ndelHashes:\n%s\nerr: %v\n", proof.String(), printHashes(leafHashes), err)
	}

	// Check roots.
	intermediate, gotRoots, err := calculateHashes(m.NumLeaves, leafHashes, proof)
	if err != nil {
		return err
	}
	if len(gotRoots) != len(rootIndexes) {
		return fmt.Errorf("expected %d calculated roots but got %d", len(gotRoots), len(rootIndexes))
	}

	for i, rootIdx := range rootIndexes {
		if haveRoots[rootIdx] != gotRoots[i] {
			return fmt.Errorf("For root position %d, calculated %s but have %s",
				rootPositions[i], hex.EncodeToString(gotRoots[i][:]),
				hex.EncodeToString(haveRoots[rootIdx][:]))
		}
	}

	// Check all intermediate nodes.
	for i, pos := range intermediate.positions {
		haveNode, found := m.Nodes[pos]
		if !found {
			continue
		}
		gotHash := intermediate.hashes[i]

		if haveNode.Hash != gotHash {
			return fmt.Errorf("For position %d, calculated %s but have %s",
				pos, hex.EncodeToString(gotHash[:]), hex.EncodeToString(haveNode.Hash[:]))
		}
	}

	return nil
}

// checkEqualProof checks that the two proofs are the same.
func (p *Proof) checkEqualProof(other Proof) error {
	if len(other.Targets) != len(p.Targets) {
		return fmt.Errorf("Have %d targets but other has %d targets. mine: %v, other: %v",
			len(p.Targets), len(other.Targets), p.Targets, other.Targets)
	}

	for i := range p.Targets {
		if p.Targets[i] != other.Targets[i] {
			return fmt.Errorf("At idx %d have %d but other has %d. sorted mine: %v, sorted other: %v",
				i, p.Targets[i], other.Targets[i], p.Targets, other.Targets)
		}
	}

	if len(other.Proof) != len(p.Proof) {
		return fmt.Errorf("Have %d proof but other has %d proof.\nMine:\n%s\nother:\n%s\n",
			len(p.Proof), len(other.Proof), printHashes(p.Proof), printHashes(other.Proof))
	}

	for i := range p.Proof {
		if p.Proof[i] != other.Proof[i] {
			return fmt.Errorf("At idx %d have %s but other has %s.\nMine:\n%s\nother:\n%s\n",
				i, p.Proof[i], other.Proof[i], printHashes(p.Proof), printHashes(other.Proof))
		}
	}

	return nil
}

func FuzzMapPollardChain(f *testing.F) {
	var tests = []struct {
		numAdds  uint32
		duration uint32
		seed     int64
	}{
		{3, 0x07, 0x07},
	}
	for _, test := range tests {
		f.Add(test.numAdds, test.duration, test.seed)
	}

	f.Fuzz(func(t *testing.T, numAdds, duration uint32, seed int64) {
		t.Parallel()

		// simulate blocks with simchain
		sc := newSimChainWithSeed(duration, seed)

		m := NewMapPollard()
		if numAdds&1 == 1 {
			m.TotalRows = 50
		}
		full := NewAccumulator(true)

		var totalAdds, totalDels int
		for b := 0; b <= 50; b++ {
			adds, _, delHashes := sc.NextBlock(numAdds)
			totalAdds += len(adds)
			totalDels += len(delHashes)

			expectProof, err := full.Prove(delHashes)
			if err != nil {
				t.Fatal(err)
			}

			err = m.Verify(delHashes, expectProof, true)
			if err != nil {
				t.Fatalf("%v\nproving delHashes:\nproof:\n%s\n%s\nmap:\n%s\nfull:\n%s\n",
					err, printHashes(delHashes), expectProof.String(),
					m.String(), full.String())
			}

			proof, err := m.Prove(delHashes)
			if err != nil {
				t.Fatalf("FuzzMapPollardChain fail at block %d. Couldn't prove\n%s\nError: %v",
					b, printHashes(delHashes), err)
			}

			err = proof.checkEqualProof(expectProof)
			if err != nil {
				t.Fatalf("\nFor delhashes: %v\nexpected proof:\n%s\ngot:\n%s\nerr: %v\n"+
					"maptreexo:\n%s\nfull:\n%s\n",
					printHashes(delHashes), expectProof.String(), proof.String(), err,
					m.String(), full.String())
			}

			for _, target := range proof.Targets {
				fetch := target
				if m.TotalRows != treeRows(m.NumLeaves) {
					fetch = translatePos(fetch, treeRows(m.NumLeaves), m.TotalRows)
				}
				hash := m.GetHash(fetch)
				if hash == empty {
					t.Fatalf("FuzzMapPollardChain doesn't have the hash "+
						"for %d at block %d.", target, b)
				}
			}

			err = m.Modify(adds, delHashes, proof)
			if err != nil {
				t.Fatalf("FuzzMapPollardChain fail at block %d. Error: %v", b, err)
			}

			err = full.Modify(adds, delHashes, proof)
			if err != nil {
				t.Fatal(err)
			}

			cachedHashes := make([]Hash, 0, len(m.CachedLeaves))
			leafHashes := make([]Hash, 0, len(m.CachedLeaves))
			for hash := range m.CachedLeaves {
				cachedHashes = append(cachedHashes, hash)
				leafHashes = append(leafHashes, hash)
			}

			if !reflect.DeepEqual(cachedHashes, leafHashes) {
				err := fmt.Errorf("Fail at block %d. For cachedLeaves of %v\ngot cachedHashes:\n%s\n"+
					"leafHashes:\n%s\nmaptreexo:\n%s\nfull:\n%s\n",
					b, m.CachedLeaves, printHashes(cachedHashes), printHashes(leafHashes),
					m.String(), full.String())
				t.Fatal(err)
			}

			cachedProofExpect, err := full.Prove(cachedHashes)
			if err != nil {
				t.Fatal(err)
			}

			cachedProof, err := m.Prove(leafHashes)
			if err != nil {
				t.Fatal(err)
			}

			if m.TotalRows != treeRows(m.NumLeaves) {
				for i := range cachedProof.Targets {
					cachedProof.Targets[i] = translatePos(
						cachedProof.Targets[i], m.TotalRows, treeRows(m.NumLeaves))
				}
			}

			err = cachedProof.checkEqualProof(cachedProofExpect)
			if err != nil {
				t.Fatalf("\nFor delhashes: %v\nexpected proof:\n%s\ngot:\n%s\nerr: %v\n"+
					"maptreexo:\n%s\nfull:\n%s\n",
					printHashes(cachedHashes), cachedProofExpect.String(), cachedProof.String(),
					err, m.String(), full.String())
			}

			fullRoots := full.GetRoots()
			mapRoots := m.GetRoots()
			if !reflect.DeepEqual(fullRoots, mapRoots) {
				t.Fatalf("Roots differ. expected:\n%s\nbut got:\n%s\nfull:\n%s\nmap:\n%s\n",
					printHashes(fullRoots), printHashes(mapRoots), full.String(), m.String())
			}

			err = m.sanityCheck()
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

func FuzzMapPollardWriteAndRead(f *testing.F) {
	var tests = []struct {
		numAdds  uint32
		duration uint32
		seed     int64
	}{
		{3, 0x07, 0x07},
	}
	for _, test := range tests {
		f.Add(test.numAdds, test.duration, test.seed)
	}

	f.Fuzz(func(t *testing.T, numAdds, duration uint32, seed int64) {
		t.Parallel()

		rand.Seed(seed)

		// simulate blocks with simchain
		sc := newSimChainWithSeed(duration, seed)

		m := NewMapPollard()
		for b := 0; b <= 20; b++ {
			adds, durations, delHashes := sc.NextBlock(numAdds)
			for i, duration := range durations {
				if duration != 0 {
					adds[i].Remember = true
				}
			}

			proof, err := m.Prove(delHashes)
			if err != nil {
				t.Fatalf("FuzzWriteAndRead fail at block %d. Error: %v", b, err)
			}

			err = m.Modify(adds, delHashes, proof)
			if err != nil {
				t.Fatalf("FuzzWriteAndRead fail at block %d. Error: %v", b, err)
			}
		}
		err := m.checkHashes()
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		wroteBytes, err := m.Write(&buf)
		if err != nil {
			t.Fatal(err)
		}

		if wroteBytes != len(buf.Bytes()) {
			t.Fatalf("FuzzWriteAndRead Fail. Wrote %d but serializeSize got %d",
				wroteBytes, len(buf.Bytes()))
		}

		m1 := NewMapPollard()

		// Restore from the buffer.
		readBytes, err := m1.Read(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if readBytes != wroteBytes {
			t.Fatalf("FuzzWriteAndRead Fail. Wrote %d but read %d", readBytes, wroteBytes)
		}

		// Check that the hashes of the roots are correct.
		err = m1.checkHashes()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzMapPollardPrune(f *testing.F) {
	var tests = []struct {
		startLeaves uint32
		modifyAdds  uint32
		delCount    uint32
	}{
		{3, 4, 1},
	}
	for _, test := range tests {
		f.Add(test.startLeaves, test.modifyAdds, test.delCount)
	}

	f.Fuzz(func(t *testing.T, startLeaves uint32, modifyAdds uint32, delCount uint32) {
		t.Parallel()

		// delCount must be less than the current number of leaves.
		if delCount >= startLeaves {
			return
		}

		// Boilerplate for generating a pollard.
		leaves, delHashes, _ := getAddsAndDels(0, startLeaves, delCount)
		acc := NewMapPollard()
		err := acc.Modify(leaves, nil, Proof{})
		if err != nil {
			t.Fatal(err)
		}
		proof, err := acc.Prove(delHashes)
		if err != nil {
			t.Fatal(err)
		}
		modifyLeaves, _, _ := getAddsAndDels(uint32(acc.GetNumLeaves()), modifyAdds, 0)
		err = acc.Modify(modifyLeaves, delHashes, proof)
		if err != nil {
			t.Fatal(err)
		}

		// Return now since we don't have anything to prune.
		if len(acc.CachedLeaves) == 0 {
			return
		}

		// Randomly choose targets to prune.
		count := rand.Intn(len(acc.CachedLeaves))
		prunedPositions := make([]uint64, 0, count)
		targets := make([]uint64, 0, len(acc.CachedLeaves))

		toPrune := make([]Hash, 0, count)
		notPruned := make([]Hash, 0, len(acc.CachedLeaves)-count)
		for k, v := range acc.CachedLeaves {
			if len(toPrune) >= count {
				targets = append(targets, v)
				notPruned = append(notPruned, k)
				continue
			}
			if rand.Int()%2 == 0 {
				toPrune = append(toPrune, k)
				prunedPositions = append(prunedPositions, v)
			} else {
				targets = append(targets, v)
				notPruned = append(notPruned, k)
			}
		}
		slices.Sort(targets)
		slices.Sort(prunedPositions)

		// Calculate the nodes that should not exist after the prune.
		shouldNotExist, _ := proofPositions(prunedPositions, acc.NumLeaves, acc.TotalRows)
		exist, _ := proofPositions(targets, acc.NumLeaves, acc.TotalRows)
		shouldNotExist = subtractSortedSlice(shouldNotExist, exist, uint64Cmp)

		// Prune the randomly chosen hashes from the accumulator.
		err = acc.Prune(toPrune)
		if err != nil {
			t.Fatal(err)
		}

		// Check that the not pruned hashes are able to be proven.
		proof, err = acc.Prove(notPruned)
		if err != nil {
			t.Fatal(err)
		}
		err = acc.Verify(notPruned, proof, false)
		if err != nil {
			t.Fatal(err)
		}

		// Check that the positions that should not exist actually don't exist.
		for _, pos := range shouldNotExist {
			_, found := acc.Nodes[pos]
			if found {
				t.Fatalf("position %d shouldn't exist", pos)
			}
		}
	})
}
