// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mvcc

import (
	"encoding/binary"
	"time"

	"go.uber.org/zap"
)

func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	totalStart := time.Now()
	plog.Infof("Starting scheduledCompaction(%v, keep); len(keep) = %v", compactMainRev, len(keep))
	defer plog.Infof("Finished scheduledCompaction(%v, keep); len(keep) = %v", compactMainRev, len(keep))
	defer func() { dbCompactionTotalMs.Observe(float64(time.Since(totalStart) / time.Millisecond)) }()
	keyCompactions := 0
	defer func() { dbCompactionKeysCounter.Add(float64(keyCompactions)) }()

	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1))

	batchsize := int64(1000)
	last := make([]byte, 8+1+8)

	compactKeys := make([][]byte, batchsize)
	for {
		plog.Infof("Starting iteration in scheduledCompaction %v; last=%v", compactMainRev, last)
		var rev revision

		start := time.Now()
		readStart := time.Now()
		rtx := s.b.ConcurrentReadTx()
		inlockStart := time.Now()
		rtx.RLock()

		compactKeys := compactKeys[:0]
		batchCompactions := 0
		// TODO: this returns keys and values, but we only need keys. Optimize?
		keys, _ := rtx.UnsafeRange(keyBucketName, last, end, batchsize)
		for _, key := range keys {
			rev = bytesToRev(key)
			if _, ok := keep[rev]; !ok {
				ckey := make([]byte, len(key))
				copy(ckey, key)
				compactKeys = append(compactKeys, ckey)
				batchCompactions++
			}
		}
		keyCompactions += batchCompactions
		// update last
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last)
		rtx.RUnlock()
		plog.Printf("finished scheduled compaction read batch of %d keys at %d (took %v; inlock: %v)", batchCompactions, compactMainRev, time.Since(readStart), time.Since(inlockStart))

		// TODO: Is it faster or slower to execute the deletes in a separate txn?
		if len(compactKeys) > 0 {
			batchStart := time.Now()
			/*tx := s.b.BatchTx()
			tx.Lock()
			for _, k := range compactKeys {
				tx.UnsafeDelete(keyBucketName, k)
			}
			tx.Unlock()*/
			err := s.b.Compact(keyBucketName, compactKeys)
			if err != nil {
				if err != nil {
					if s.lg != nil {
						s.lg.Error(
							"compaction failed",
							zap.Int64("compact-revision", compactMainRev),
						)
					} else {
						plog.Errorf("compaction failed for %d: %v", compactMainRev, err)
					}
				}
			}
			if s.lg != nil {
				s.lg.Info(
					"finished scheduled compaction batch",
					zap.Int64("compact-revision", compactMainRev),
					zap.Int("key-count", batchCompactions),
					zap.Duration("took", time.Since(totalStart)),
				)
			} else {
				plog.Printf("finished scheduled compaction batch of %d keys at %d (took %v)", batchCompactions, compactMainRev, time.Since(batchStart))
			}
		}

		if len(keys) < int(batchsize) {
			tx := s.b.BatchTx()
			tx.Lock()
			rbytes := make([]byte, 8+1+8)
			revToBytes(revision{main: compactMainRev}, rbytes)
			tx.UnsafePut(metaBucketName, finishedCompactKeyName, rbytes)
			tx.Unlock()
			if s.lg != nil {
				s.lg.Info(
					"finished scheduled compaction",
					zap.Int64("compact-revision", compactMainRev),
					zap.Int("key-count", keyCompactions),
					zap.Duration("took", time.Since(totalStart)),
				)
			} else {
				plog.Printf("finished scheduled compaction of %d keys at %d (took %v)", keyCompactions, compactMainRev, time.Since(totalStart))
			}
			return true
		}

		dbCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))

		plog.Infof("Finished iteration in scheduledCompaction %v; last=%v", compactMainRev, last)
		select {
		case <-time.After(10 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}
