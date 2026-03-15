package dsl

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"anhebridgedb/internal/db"
)

type Executor struct {
	engine *db.Engine
}

func New(engine *db.Engine) *Executor {
	return &Executor{engine: engine}
}

func (e *Executor) Execute(input string) ([]any, error) {
	statements := splitStatements(input)
	results := make([]any, 0, len(statements))
	for _, stmt := range statements {
		result, err := e.executeOne(stmt)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (e *Executor) executeOne(statement string) (any, error) {
	stmt := strings.TrimSpace(statement)
	switch {
	case strings.HasPrefix(strings.ToUpper(stmt), "ASET "):
		return e.executeASet(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "AGET "):
		return e.executeAGet(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "SET "):
		return e.executeSet(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "GET "):
		return e.executeGet(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "SEARCH "):
		return e.executeSearch(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "DELETE "):
		return e.executeDelete(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "ROLLBACK "):
		return e.executeRollback(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "CHECK "):
		return e.executeCheck(stmt)
	case strings.EqualFold(stmt, "VERIFY STORAGE"):
		return e.engine.VerifyStorage()
	case strings.EqualFold(stmt, "COMPACT STORAGE"):
		return e.engine.CompactStorage()
	case strings.HasPrefix(strings.ToUpper(stmt), "CREATE RULE "):
		return e.executeCreateRule(stmt)
	case strings.EqualFold(stmt, "SHOW METRICS"):
		return e.engine.Metrics(), nil
	case strings.EqualFold(stmt, "SHOW PERF"):
		return e.engine.DebugPerf(), nil
	case strings.EqualFold(stmt, "SHOW RULES"):
		return e.engine.ListRules(), nil
	case strings.HasPrefix(strings.ToUpper(stmt), "SHOW RULE "):
		id := strings.TrimSpace(stmt[len("SHOW RULE "):])
		return e.engine.GetRule(id)
	case strings.EqualFold(stmt, "RUN SCHEDULER"):
		return e.engine.ProcessDueTasks(time.Now().UTC())
	default:
		return nil, fmt.Errorf("unsupported statement: %s", stmt)
	}
}

func (e *Executor) executeSet(stmt string) (any, error) {
	parts := strings.SplitN(stmt, " ", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid SET statement")
	}
	payload, err := parseSetPayload(parts[2])
	if err != nil {
		return nil, err
	}
	if payload.IdempotencyKey != "" {
		return e.engine.SetWithIdempotencyKey(parts[1], payload.Value, payload.EventName, payload.IdempotencyKey)
	}
	if payload.EventName != "" {
		return e.engine.SetWithEventName(parts[1], payload.Value, payload.EventName)
	}
	return e.engine.Set(parts[1], payload.Value)
}

func (e *Executor) executeASet(stmt string) (any, error) {
	payload := strings.TrimSpace(stmt[len("ASET "):])
	itemsText, err := splitCommaSegments(payload)
	if err != nil {
		return nil, err
	}

	items := make([]db.BatchSetItem, 0, len(itemsText))
	for _, itemText := range itemsText {
		itemText = strings.TrimSpace(itemText)
		if strings.HasPrefix(strings.ToUpper(itemText), "SET ") {
			itemText = strings.TrimSpace(itemText[len("SET "):])
		}
		parts := strings.SplitN(itemText, " ", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid ASET item: %s", itemText)
		}
		value, err := parseSetPayload(parts[1])
		if err != nil {
			return nil, err
		}
		items = append(items, db.BatchSetItem{
			Key:            parts[0],
			Value:          value.Value,
			EventName:      value.EventName,
			IdempotencyKey: value.IdempotencyKey,
		})
	}
	return e.engine.BatchSet(items)
}

func (e *Executor) executeAGet(stmt string) (any, error) {
	payload := strings.TrimSpace(stmt[len("AGET "):])
	if payload == "" {
		return nil, fmt.Errorf("AGET requires at least one key")
	}
	payload = strings.ReplaceAll(payload, ",", " ")
	keys := strings.Fields(payload)
	return e.engine.BatchGet(keys), nil
}

func (e *Executor) executeGet(stmt string) (any, error) {
	if strings.Contains(strings.ToUpper(stmt), "ALLTIME") {
		return e.executeTimelineQuery(stmt)
	}
	timelineWithDiff := regexp.MustCompile(`(?i)^GET\s+(\S+)\s+ALLTIME\s+WITH\s+DIFF$`)
	if matches := timelineWithDiff.FindStringSubmatch(stmt); len(matches) == 2 {
		return e.engine.Timeline(matches[1], true)
	}

	timeline := regexp.MustCompile(`(?i)^GET\s+(\S+)\s+ALLTIME$`)
	if matches := timeline.FindStringSubmatch(stmt); len(matches) == 2 {
		return e.engine.Timeline(matches[1], false)
	}

	at := regexp.MustCompile(`(?i)^GET\s+(\S+)\s+AT\s+'([^']+)'$`)
	if matches := at.FindStringSubmatch(stmt); len(matches) == 3 {
		ts, err := time.Parse(time.RFC3339, matches[2])
		if err != nil {
			return nil, err
		}
		return e.engine.GetAt(matches[1], ts.UTC())
	}

	last := regexp.MustCompile(`(?i)^GET\s+(\S+)\s+LAST(?:\s+(.+))?$`)
	if matches := last.FindStringSubmatch(stmt); len(matches) == 3 {
		steps, err := parseLastSteps(matches[2])
		if err != nil {
			return nil, err
		}
		return e.engine.GetLast(matches[1], steps)
	}

	latest := regexp.MustCompile(`(?i)^GET\s+(\S+)$`)
	if matches := latest.FindStringSubmatch(stmt); len(matches) == 2 {
		return e.engine.Get(matches[1])
	}

	return nil, fmt.Errorf("invalid GET statement")
}

func (e *Executor) executeTimelineQuery(stmt string) (any, error) {
	re := regexp.MustCompile(`(?i)^GET\s+(\S+)\s+ALLTIME(?:\s+WITH\s+DIFF)?(?:\s+LIMIT\s+(\d+))?(?:\s+BEFORE\s+VERSION:(\d+))?(?:\s+AFTER\s+VERSION:(\d+))?$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 5 {
		return nil, fmt.Errorf("invalid GET statement")
	}
	withDiff := strings.Contains(strings.ToUpper(stmt), "WITH DIFF")
	limit, before, after, err := parseWindowParts(matches[2], matches[3], matches[4])
	if err != nil {
		return nil, err
	}
	return e.engine.TimelineWindow(matches[1], withDiff, limit, before, after)
}

func (e *Executor) executeDelete(stmt string) (any, error) {
	parts := strings.Fields(stmt)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid DELETE statement")
	}
	return e.engine.Delete(parts[1])
}

func (e *Executor) executeSearch(stmt string) (any, error) {
	options, err := parseSearchOptions(stmt)
	if err != nil {
		return nil, err
	}
	return e.engine.SearchEvents(options)
}

func (e *Executor) executeCreateRule(stmt string) (any, error) {
	re := regexp.MustCompile(`(?is)^CREATE\s+RULE\s+([A-Za-z0-9_\-]+)\s+ON\s+PATTERN\s+"([^"]+)"\s+IF\s+UNCHANGED\s+FOR\s+([^\s]+)\s+THEN\s+TRANSITION\s+TO\s+"([^"]+)"$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 5 {
		return nil, fmt.Errorf("invalid CREATE RULE statement")
	}
	return e.engine.CreateRule(db.RuleSpec{
		ID:      matches[1],
		Pattern: matches[2],
		Delay:   matches[3],
		Target:  matches[4],
	})
}

func (e *Executor) executeCheck(stmt string) (any, error) {
	re := regexp.MustCompile(`(?i)^CHECK\s+(\S+)\s+ALLTIME(?:\s+LIMIT\s+(\d+))?(?:\s+BEFORE\s+VERSION:(\d+))?(?:\s+AFTER\s+VERSION:(\d+))?$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 5 {
		return nil, fmt.Errorf("invalid CHECK statement")
	}
	limit, before, after, err := parseWindowParts(matches[2], matches[3], matches[4])
	if err != nil {
		return nil, err
	}
	return e.engine.CheckAllTimeWindow(matches[1], limit, before, after)
}

func (e *Executor) executeRollback(stmt string) (any, error) {
	lastRe := regexp.MustCompile(`(?i)^ROLLBACK\s+(\S+)\s+VERSION:LAST$`)
	if matches := lastRe.FindStringSubmatch(stmt); len(matches) == 2 {
		return e.engine.RollbackLast(matches[1])
	}

	re := regexp.MustCompile(`(?i)^ROLLBACK\s+(\S+)\s+VERSION:(\d+)$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid ROLLBACK statement")
	}

	var version uint64
	if _, err := fmt.Sscanf(matches[2], "%d", &version); err != nil {
		return nil, fmt.Errorf("invalid rollback version")
	}
	return e.engine.Rollback(matches[1], version)
}

func splitStatements(input string) []string {
	parts := strings.Split(input, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			statements = append(statements, trimmed)
		}
	}
	return statements
}

func parseWindowParts(limitPart, beforePart, afterPart string) (int, uint64, uint64, error) {
	limit := 0
	before := uint64(0)
	after := uint64(0)
	if limitPart != "" {
		if _, err := fmt.Sscanf(limitPart, "%d", &limit); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid limit")
		}
	}
	if beforePart != "" {
		if _, err := fmt.Sscanf(beforePart, "%d", &before); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid before version")
		}
	}
	if afterPart != "" {
		if _, err := fmt.Sscanf(afterPart, "%d", &after); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid after version")
		}
	}
	return limit, before, after, nil
}

func parseJSONOrScalar(value string) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(value)
	if json.Valid([]byte(trimmed)) {
		return json.RawMessage(trimmed), nil
	}

	bytes, err := json.Marshal(trimmed)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

type setPayload struct {
	Value          json.RawMessage
	EventName      string
	IdempotencyKey string
}

func parseSetPayload(input string) (setPayload, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return setPayload{}, fmt.Errorf("missing SET value")
	}

	payload := setPayload{}
	rest := trimmed
	for i := 0; i < 2; i++ {
		prefix, token, ok := extractTrailingToken(rest)
		if !ok {
			break
		}
		switch {
		case payload.IdempotencyKey == "" && isIdempotencyToken(token):
			payload.IdempotencyKey = parseIdempotencyToken(token)
			rest = prefix
		case payload.EventName == "" && isEventName(token):
			payload.EventName = token
			rest = prefix
		default:
			goto parseValue
		}
	}

parseValue:
	value, err := parseJSONOrScalar(rest)
	if err != nil {
		return setPayload{}, err
	}
	payload.Value = value
	return payload, nil
}

func isEventName(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' {
			continue
		}
		if i > 0 && ((r >= '0' && r <= '9') || r == '-') {
			continue
		}
		return false
	}
	return true
}

func parseLastSteps(input string) (int, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return 1, nil
	}

	parts := strings.Fields(trimmed)
	steps := 1
	for _, part := range parts {
		if part != "-1" {
			return 0, fmt.Errorf("LAST only supports repeated -1 steps")
		}
		steps++
	}
	return steps, nil
}

func splitCommaSegments(input string) ([]string, error) {
	var (
		segments []string
		start    int
		depth    int
		inQuote  bool
		escape   bool
	)

	for i, r := range input {
		switch {
		case escape:
			escape = false
		case r == '\\':
			escape = true
		case r == '"':
			inQuote = !inQuote
		case !inQuote && (r == '{' || r == '['):
			depth++
		case !inQuote && (r == '}' || r == ']'):
			if depth > 0 {
				depth--
			}
		case !inQuote && depth == 0 && r == ',':
			segment := strings.TrimSpace(input[start:i])
			if segment != "" {
				segments = append(segments, segment)
			}
			start = i + 1
		}
	}

	tail := strings.TrimSpace(input[start:])
	if tail != "" {
		segments = append(segments, tail)
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("empty batch payload")
	}
	return segments, nil
}

func parseSearchOptions(stmt string) (db.SearchOptions, error) {
	parts := strings.Fields(strings.TrimSpace(stmt))
	if len(parts) < 2 || !strings.EqualFold(parts[0], "SEARCH") || !strings.EqualFold(parts[1], "EVENTS") {
		return db.SearchOptions{}, fmt.Errorf("invalid SEARCH statement")
	}
	options := db.SearchOptions{Desc: false, Limit: 50, Page: 1}
	for i := 2; i < len(parts); i++ {
		part := parts[i]
		upper := strings.ToUpper(part)
		switch {
		case upper == "WITH":
			if i+2 >= len(parts) || !strings.EqualFold(parts[i+1], "SAME") || !strings.EqualFold(parts[i+2], "I") {
				return db.SearchOptions{}, fmt.Errorf("invalid WITH clause")
			}
			options.WithSameI = true
			i += 2
		case upper == "DESC":
			options.Desc = true
		case upper == "ASC":
			options.Desc = false
		case upper == "LIMIT":
			if i+1 >= len(parts) {
				return db.SearchOptions{}, fmt.Errorf("missing LIMIT value")
			}
			i++
			_, err := fmt.Sscanf(parts[i], "%d", &options.Limit)
			if err != nil {
				return db.SearchOptions{}, fmt.Errorf("invalid LIMIT value")
			}
		case upper == "PAGE":
			if i+1 >= len(parts) {
				return db.SearchOptions{}, fmt.Errorf("missing PAGE value")
			}
			i++
			_, err := fmt.Sscanf(parts[i], "%d", &options.Page)
			if err != nil {
				return db.SearchOptions{}, fmt.Errorf("invalid PAGE value")
			}
		case hasTokenPrefix(part, "KEY"):
			options.Key = parseTokenValue(part)
		case hasTokenPrefix(part, "NAME"), hasTokenPrefix(part, "EVENT_NAME"), hasTokenPrefix(part, "E"):
			options.EventName = parseTokenValue(part)
		case hasTokenPrefix(part, "IDEMPOTENCY_KEY"), hasTokenPrefix(part, "I"):
			options.IdempotencyKey = parseTokenValue(part)
		case hasTokenPrefix(part, "LIMIT"):
			_, err := fmt.Sscanf(parseTokenValue(part), "%d", &options.Limit)
			if err != nil {
				return db.SearchOptions{}, fmt.Errorf("invalid LIMIT value")
			}
		case hasTokenPrefix(part, "PAGE"):
			_, err := fmt.Sscanf(parseTokenValue(part), "%d", &options.Page)
			if err != nil {
				return db.SearchOptions{}, fmt.Errorf("invalid PAGE value")
			}
		default:
			return db.SearchOptions{}, fmt.Errorf("unsupported SEARCH token: %s", part)
		}
	}
	if options.Page < 1 {
		options.Page = 1
	}
	if options.Limit < 1 {
		options.Limit = 50
	}
	return options, nil
}

func extractTrailingToken(input string) (string, string, bool) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", "", false
	}
	inQuote := false
	escape := false
	depth := 0
	for i := len(trimmed) - 1; i >= 0; i-- {
		r := rune(trimmed[i])
		switch {
		case escape:
			escape = false
		case r == '\\':
			escape = true
		case r == '"':
			inQuote = !inQuote
		case !inQuote && (r == '}' || r == ']'):
			depth++
		case !inQuote && (r == '{' || r == '['):
			if depth > 0 {
				depth--
			}
		case !inQuote && depth == 0 && (r == ' ' || r == '\t'):
			return strings.TrimSpace(trimmed[:i]), strings.TrimSpace(trimmed[i+1:]), true
		}
	}
	return "", trimmed, true
}

func isIdempotencyToken(token string) bool {
	return hasTokenPrefix(token, "I") || hasTokenPrefix(token, "IDEMPOTENCY")
}

func parseIdempotencyToken(token string) string {
	return parseTokenValue(token)
}

func hasTokenPrefix(token, prefix string) bool {
	upper := strings.ToUpper(token)
	return strings.HasPrefix(upper, prefix+":") || strings.HasPrefix(upper, prefix+"=")
}

func parseTokenValue(token string) string {
	if idx := strings.IndexAny(token, ":="); idx >= 0 {
		return token[idx+1:]
	}
	return ""
}
