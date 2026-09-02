package main

import (
	"bytes"
	"encoding/json"
	"math/big"
	"reflect"
	"strings"
	"sync"
)

// Cache stores messages for replaying to late-joining frontends.
// Streaming chunks (agent_message_chunk, agent_thought_chunk) are
// coalesced into single messages so replay is compact and fast.
type Cache struct {
	mu       sync.Mutex
	meta     []byte   // optional session metadata (name, etc.)
	initResp []byte   // cached initialize response
	newResp  []byte   // cached session/new response
	updates  [][]byte // coalesced session/update notifications

	// Pending permission request from the agent (reverse call).
	// Stored so late-joining frontends can see and respond to it.
	// Cleared when a response is sent back.
	pendingPermission []byte

	// Accumulator for the current run of chunks
	chunkType     string // "agent_message_chunk" or "agent_thought_chunk", or ""
	chunkText     strings.Builder
	chunkEnvelope map[string]interface{}
}

func NewCache() *Cache {
	return &Cache{}
}

func (c *Cache) SetMeta(line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.meta = append([]byte(nil), line...)
}

func (c *Cache) SetInitResponse(line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initResp = append([]byte(nil), line...)
}

func (c *Cache) SetNewResponse(line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.newResp = append([]byte(nil), line...)
}

func (c *Cache) SetPendingPermission(line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingPermission = append([]byte(nil), line...)
}

func (c *Cache) ClearPendingPermission() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Move answered permission into updates so it replays as history
	// (e.g. plan approval cards remain visible in replay).
	if c.pendingPermission != nil {
		c.flushChunks()
		c.updates = append(c.updates, c.pendingPermission)
		c.pendingPermission = nil
	}
}

func (c *Cache) AddUpdate(line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Parse the update type and content
	kind, text, _ := parseUpdateType(line)
	envelope, _ := decodeJSONMap(line)
	if !isEligibleTextChunk(envelope, kind) {
		kind = ""
	}

	switch kind {
	case "agent_message_chunk", "agent_thought_chunk":
		if c.chunkType == kind &&
			chunkEnvelopesEquivalent(c.chunkEnvelope, envelope) {
			// Same logical text stream — accumulate.
			c.chunkText.WriteString(text)
		} else {
			// Different type — flush previous, start new
			c.flushChunks()
			c.chunkType = kind
			c.chunkText.WriteString(text)
			c.chunkEnvelope = envelope
		}
	default:
		// Non-chunk update — flush any pending chunks, then store
		c.flushChunks()
		c.updates = append(c.updates, append([]byte(nil), line...))
	}
}

// flushChunks coalesces accumulated chunks into a single notification.
// Must be called with c.mu held.
func (c *Cache) flushChunks() {
	if c.chunkType == "" {
		return
	}

	params, _ := c.chunkEnvelope["params"].(map[string]interface{})
	update, _ := params["update"].(map[string]interface{})
	content, _ := update["content"].(map[string]interface{})
	content["text"] = c.chunkText.String()

	if line, err := json.Marshal(c.chunkEnvelope); err == nil {
		c.updates = append(c.updates, line)
	}

	c.chunkType = ""
	c.chunkText.Reset()
	c.chunkEnvelope = nil
}

func decodeJSONMap(line []byte) (map[string]interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	var envelope map[string]interface{}
	if err := decoder.Decode(&envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}

func isEligibleTextChunk(envelope map[string]interface{}, kind string) bool {
	if kind != "agent_message_chunk" && kind != "agent_thought_chunk" {
		return false
	}
	if method, _ := envelope["method"].(string); method != "session/update" {
		return false
	}
	params, ok := envelope["params"].(map[string]interface{})
	if !ok {
		return false
	}
	sessionID, ok := params["sessionId"].(string)
	if !ok || sessionID == "" {
		return false
	}
	update, ok := params["update"].(map[string]interface{})
	if !ok {
		return false
	}
	updateKind, ok := update["sessionUpdate"].(string)
	if !ok || updateKind != kind {
		return false
	}
	if messageID, present := update["messageId"]; present {
		messageIDString, ok := messageID.(string)
		if !ok || messageIDString == "" {
			return false
		}
	}
	content, ok := update["content"].(map[string]interface{})
	if !ok || content["type"] != "text" {
		return false
	}
	_, ok = content["text"].(string)
	return ok
}

func chunkEnvelopesEquivalent(first, next map[string]interface{}) bool {
	return jsonValuesEquivalent(envelopeWithoutChunkText(first), envelopeWithoutChunkText(next))
}

func jsonValuesEquivalent(first, next interface{}) bool {
	switch firstValue := first.(type) {
	case map[string]interface{}:
		nextValue, ok := next.(map[string]interface{})
		if !ok || len(firstValue) != len(nextValue) {
			return false
		}
		for key, firstChild := range firstValue {
			nextChild, exists := nextValue[key]
			if !exists || !jsonValuesEquivalent(firstChild, nextChild) {
				return false
			}
		}
		return true
	case []interface{}:
		nextValue, ok := next.([]interface{})
		if !ok || len(firstValue) != len(nextValue) {
			return false
		}
		for index := range firstValue {
			if !jsonValuesEquivalent(firstValue[index], nextValue[index]) {
				return false
			}
		}
		return true
	case json.Number:
		nextValue, ok := next.(json.Number)
		if !ok {
			return false
		}
		firstNumber, firstOK := new(big.Rat).SetString(firstValue.String())
		nextNumber, nextOK := new(big.Rat).SetString(nextValue.String())
		if firstOK && nextOK {
			return firstNumber.Cmp(nextNumber) == 0
		}
		return firstValue.String() == nextValue.String()
	default:
		return reflect.DeepEqual(first, next)
	}
}

func envelopeWithoutChunkText(envelope map[string]interface{}) map[string]interface{} {
	copyMap := func(source map[string]interface{}) map[string]interface{} {
		result := make(map[string]interface{}, len(source))
		for key, value := range source {
			result[key] = value
		}
		return result
	}

	result := copyMap(envelope)
	params, _ := envelope["params"].(map[string]interface{})
	paramsCopy := copyMap(params)
	result["params"] = paramsCopy
	update, _ := params["update"].(map[string]interface{})
	updateCopy := copyMap(update)
	paramsCopy["update"] = updateCopy
	content, _ := update["content"].(map[string]interface{})
	contentCopy := copyMap(content)
	delete(contentCopy, "text")
	updateCopy["content"] = contentCopy
	return result
}

// Snapshot returns the cached session history in replay order.
func (c *Cache) Snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Flush any in-progress chunks so replay is complete
	c.flushChunks()

	lines := make([][]byte, 0, 3+len(c.updates)+1)
	appendLine := func(line []byte) {
		if line != nil {
			lines = append(lines, append([]byte(nil), line...))
		}
	}
	appendLine(c.meta)
	appendLine(c.initResp)
	appendLine(c.newResp)
	for _, update := range c.updates {
		appendLine(update)
	}
	appendLine(c.pendingPermission)
	return lines
}

// Replay sends the cached session history to a frontend.
func (c *Cache) Replay(f *Frontend) {
	for _, line := range c.Snapshot() {
		if !f.Send(line) {
			return
		}
	}
}

// parseUpdateType extracts the sessionUpdate type and text content from a notification.
func parseUpdateType(line []byte) (kind, text, sessionID string) {
	var msg struct {
		Params struct {
			SessionID string `json:"sessionId"`
			Update    struct {
				SessionUpdate string `json:"sessionUpdate"`
				Content       struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"update"`
		} `json:"params"`
	}
	if err := json.Unmarshal(line, &msg); err != nil {
		return "", "", ""
	}
	return msg.Params.Update.SessionUpdate, msg.Params.Update.Content.Text, msg.Params.SessionID
}
