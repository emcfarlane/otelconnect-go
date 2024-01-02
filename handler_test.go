// Copyright 2022-2023 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otelconnect

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBodyWrapper(t *testing.T) {
	t.Parallel()
	payload := []byte(`{"number": 42}`)
	prefix := [5]byte{}
	prefix[0] = 1
	binary.BigEndian.PutUint32(prefix[1:5], uint32(len(payload)))
	t.Run("single", func(t *testing.T) {
		buf := &bytes.Buffer{}
		buf.Write(prefix[:])
		buf.Write(payload)

		r := &bodyWrapper{
			base:     io.NopCloser(buf),
			protocol: grpcStreamProtocol,
		}
		got, err := io.ReadAll(r)
		assert.Nil(t, err)
		assert.Len(t, got, len(payload)+5)
		assert.Equal(t, prefix[:], got[0:5])
		assert.Equal(t, payload, got[5:])
		t.Logf("got: %s", got)
	})
	t.Run("multiple", func(t *testing.T) {
		buf := &bytes.Buffer{}
		buf.Write(prefix[:])
		buf.Write(payload)
		buf.Write(prefix[:])
		buf.Write(payload)

		r := &bodyWrapper{
			base:     io.NopCloser(buf),
			protocol: grpcStreamProtocol,
		}
		got, err := io.ReadAll(r)
		assert.Nil(t, err)
		assert.Len(t, got, 2*(len(payload)+5))
		assert.Equal(t, prefix[:], got[0:5])
		assert.Equal(t, payload, got[5:5+len(payload)])
		t.Logf("got: %s", got)
	})
}
