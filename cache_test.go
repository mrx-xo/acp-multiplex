package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"testing"
)

func replayMessages(t *testing.T, cache *Cache) []map[string]interface{} {
	t.Helper()

	var out bytes.Buffer
	cache.Replay(&Frontend{
		id:     1,
		writer: &out,
		done:   make(chan struct{}),
	})

	var messages []map[string]interface{}
	scanner := bufio.NewScanner(&out)
	for scanner.Scan() {
		var message map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			t.Fatalf("decode replayed message: %v", err)
		}
		messages = append(messages, message)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan replayed messages: %v", err)
	}
	return messages
}

func TestCacheCoalescesIdentifiedThoughtAndPreservesEnvelope(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","trace":"keep-me","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","_meta":{"provider":"codex"},"content":{"type":"text","text":"**Inspecting"}}}}`))
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","trace":"keep-me","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","_meta":{"provider":"codex"},"content":{"type":"text","text":" the renderer**"}}}}`))

	messages := replayMessages(t, cache)
	if len(messages) != 1 {
		t.Fatalf("replayed %d messages, want 1", len(messages))
	}

	want := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "session/update",
		"params": map[string]interface{}{
			"sessionId": "s1",
			"trace":     "keep-me",
			"update": map[string]interface{}{
				"sessionUpdate": "agent_thought_chunk",
				"messageId":     "thought-1",
				"_meta":         map[string]interface{}{"provider": "codex"},
				"content": map[string]interface{}{
					"type": "text",
					"text": "**Inspecting the renderer**",
				},
			},
		},
	}

	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("encode expectation: %v", err)
	}
	gotJSON, err := json.Marshal(messages[0])
	if err != nil {
		t.Fatalf("encode replayed message: %v", err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("replayed envelope:\n got: %s\nwant: %s", gotJSON, wantJSON)
	}
}

func TestCacheStillCoalescesAgentMessageChunks(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Hello, "}}}}`))
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"world."}}}}`))

	messages := replayMessages(t, cache)
	if len(messages) != 1 {
		t.Fatalf("replayed %d messages, want one coalesced agent message", len(messages))
	}
	params := messages[0]["params"].(map[string]interface{})
	update := params["update"].(map[string]interface{})
	content := update["content"].(map[string]interface{})
	if got := content["text"]; got != "Hello, world." {
		t.Fatalf("coalesced agent text = %v, want Hello, world.", got)
	}
}

func TestCacheSeparatesThoughtsWithDifferentMessageIDs(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"text","text":"first"}}}}`))
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-2","content":{"type":"text","text":"second"}}}}`))

	messages := replayMessages(t, cache)
	if len(messages) != 2 {
		t.Fatalf("replayed %d messages, want 2 distinct thought messages", len(messages))
	}
	for i, wantID := range []string{"thought-1", "thought-2"} {
		params := messages[i]["params"].(map[string]interface{})
		update := params["update"].(map[string]interface{})
		if got := update["messageId"]; got != wantID {
			t.Errorf("message %d id = %v, want %q", i, got, wantID)
		}
	}
}

func TestCacheSeparatesChunksWhenMetadataChanges(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","_meta":{"phase":"first","nested":{"a":1,"b":2}},"content":{"type":"text","text":"one"}}}}`))
	cache.AddUpdate([]byte(`{"method":"session/update","jsonrpc":"2.0","params":{"update":{"content":{"text":"two","type":"text"},"_meta":{"nested":{"b":2,"a":1},"phase":"second"},"messageId":"thought-1","sessionUpdate":"agent_thought_chunk"},"sessionId":"s1"}}`))

	messages := replayMessages(t, cache)
	if len(messages) != 2 {
		t.Fatalf("replayed %d messages, want a boundary when metadata changes", len(messages))
	}
	for i, wantPhase := range []string{"first", "second"} {
		params := messages[i]["params"].(map[string]interface{})
		update := params["update"].(map[string]interface{})
		meta := update["_meta"].(map[string]interface{})
		if got := meta["phase"]; got != wantPhase {
			t.Errorf("message %d metadata phase = %v, want %q", i, got, wantPhase)
		}
	}
}

func TestCacheDoesNotFoldNonTextContentIntoTextAccumulator(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"text","text":"caption"}}}}`))
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"image","url":"https://example.test/image.png"}}}}`))

	messages := replayMessages(t, cache)
	if len(messages) != 2 {
		t.Fatalf("replayed %d messages, want text and non-text updates kept separately", len(messages))
	}
	params := messages[1]["params"].(map[string]interface{})
	update := params["update"].(map[string]interface{})
	content := update["content"].(map[string]interface{})
	if got := content["type"]; got != "image" {
		t.Fatalf("non-text content type = %v, want image", got)
	}
	if got := content["url"]; got != "https://example.test/image.png" {
		t.Fatalf("non-text content URL = %v, want original URL", got)
	}
	if _, added := content["text"]; added {
		t.Fatal("non-text content was rewritten with a text field")
	}
}

func TestCacheCoalescesStructurallyEquivalentAnonymousChunks(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","_meta":{"nested":{"a":1,"b":2}},"content":{"type":"text","text":"one"}}}}`))
	cache.AddUpdate([]byte(`{"method":"session/update","params":{"update":{"content":{"text":"two","type":"text"},"_meta":{"nested":{"b":2,"a":1}},"sessionUpdate":"agent_thought_chunk"},"sessionId":"s1"},"jsonrpc":"2.0"}`))

	messages := replayMessages(t, cache)
	if len(messages) != 1 {
		t.Fatalf("replayed %d messages, want structurally equivalent chunks coalesced", len(messages))
	}
	params := messages[0]["params"].(map[string]interface{})
	update := params["update"].(map[string]interface{})
	content := update["content"].(map[string]interface{})
	if got := content["text"]; got != "onetwo" {
		t.Fatalf("coalesced text = %v, want onetwo", got)
	}
}

func TestCacheSeparatesIdentifiedAndAnonymousChunks(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"text","text":"identified"}}}}`))
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"anonymous"}}}}`))

	if got := len(replayMessages(t, cache)); got != 2 {
		t.Fatalf("replayed %d messages, want identified and anonymous chunks separated", got)
	}
}

func TestCacheSeparatesSameMessageIDAcrossSessions(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"text","text":"one"}}}}`))
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s2","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"text","text":"two"}}}}`))

	if got := len(replayMessages(t, cache)); got != 2 {
		t.Fatalf("replayed %d messages, want a session boundary", got)
	}
}

func TestCacheFlushesTextBeforeUnchangedNonChunkUpdate(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"text","text":"caption"}}}}`))
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"tool-1","title":"Inspect"}}}`))

	messages := replayMessages(t, cache)
	if len(messages) != 2 {
		t.Fatalf("replayed %d messages, want thought then tool call", len(messages))
	}
	params := messages[1]["params"].(map[string]interface{})
	update := params["update"].(map[string]interface{})
	if got := update["toolCallId"]; got != "tool-1" {
		t.Fatalf("tool update changed during replay: toolCallId = %v", got)
	}
}

func TestCachePreservesLargeNumericExtensionFields(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","_meta":{"sequence":9007199254740993},"content":{"type":"text","text":"caption"}}}}`))

	var out bytes.Buffer
	cache.Replay(&Frontend{id: 1, writer: &out, done: make(chan struct{})})
	if !bytes.Contains(out.Bytes(), []byte(`"sequence":9007199254740993`)) {
		t.Fatalf("large numeric extension field changed during replay: %s", out.Bytes())
	}
}

func TestCacheCoalescesEquivalentNumericEncodings(t *testing.T) {
	cache := NewCache()
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","_meta":{"ratio":1},"content":{"type":"text","text":"one"}}}}`))
	cache.AddUpdate([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","_meta":{"ratio":1.0},"content":{"type":"text","text":"two"}}}}`))

	messages := replayMessages(t, cache)
	if len(messages) != 1 {
		t.Fatalf("replayed %d messages, want numerically equivalent envelopes coalesced", len(messages))
	}
	params := messages[0]["params"].(map[string]interface{})
	update := params["update"].(map[string]interface{})
	content := update["content"].(map[string]interface{})
	if got := content["text"]; got != "onetwo" {
		t.Fatalf("coalesced text = %v, want onetwo", got)
	}
}

func TestCacheDoesNotCoalesceMalformedTextChunks(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{
			name:   "missing session ID",
			first:  `{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"one"}}}}`,
			second: `{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"two"}}}}`,
		},
		{
			name:   "empty session ID",
			first:  `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"one"}}}}`,
			second: `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"two"}}}}`,
		},
		{
			name:   "non-string session ID",
			first:  `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":7,"update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"one"}}}}`,
			second: `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":7,"update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"two"}}}}`,
		},
		{
			name:   "non-string message ID",
			first:  `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":7,"content":{"type":"text","text":"one"}}}}`,
			second: `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":7,"content":{"type":"text","text":"two"}}}}`,
		},
		{
			name:   "empty message ID",
			first:  `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"","content":{"type":"text","text":"one"}}}}`,
			second: `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","messageId":"","content":{"type":"text","text":"two"}}}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := NewCache()
			cache.AddUpdate([]byte(test.first))
			cache.AddUpdate([]byte(test.second))

			messages := replayMessages(t, cache)
			if len(messages) != 2 {
				t.Fatalf("replayed %d messages, want malformed chunks preserved separately", len(messages))
			}
			for index, wantText := range []string{"one", "two"} {
				params := messages[index]["params"].(map[string]interface{})
				update := params["update"].(map[string]interface{})
				content := update["content"].(map[string]interface{})
				if got := content["text"]; got != wantText {
					t.Errorf("message %d text = %v, want %q", index, got, wantText)
				}
			}
		})
	}
}
