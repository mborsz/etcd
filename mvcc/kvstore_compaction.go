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

	batchsize := int64(10000)
	last := make([]byte, 8+1+8)
	for {
		plog.Infof("Starting iteration in scheduledCompaction %v; last=%v", compactMainRev, last)
		var rev revision

		start := time.Now()
		tx := s.b.BatchTx()
		plog.Infof("Attempt to tx.Lock in %v %v", compactMainRev, last)
		tx.Lock()
		plog.Infof("tx.Lock in %v %v", compactMainRev, last)

		keys, _ := tx.UnsafeRange(keyBucketName, last, end, batchsize)
		for _, key := range keys {
			rev = bytesToRev(key)
			if _, ok := keep[rev]; !ok {
				tx.UnsafeDelete(keyBucketName, key)
				keyCompactions++
			}
		}

		if len(keys) < int(batchsize) {
			rbytes := make([]byte, 8+1+8)
			revToBytes(revision{main: compactMainRev}, rbytes)
			tx.UnsafePut(metaBucketName, finishedCompactKeyName, rbytes)
			tx.Unlock()
			if s.lg != nil {
				s.lg.Info(
					"finished scheduled compaction",
					zap.Int64("compact-revision", compactMainRev),
					zap.Duration("took", time.Since(totalStart)),
				)
			} else {
				plog.Printf("finished scheduled compaction at %d (took %v)", compactMainRev, time.Since(totalStart))
			}
			return true
		}

		// update last
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last)
		plog.Infof("tx.Unlocking in %v %v", compactMainRev, last)
		tx.Unlock()
		plog.Infof("tx.Unlocked in %v %v", compactMainRev, last)
		dbCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))

		plog.Infof("Finished iteration in scheduledCompaction %v; last=%v", compactMainRev, last)
		select {
		case <-time.After(100 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}
