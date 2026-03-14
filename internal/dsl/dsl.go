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
	case strings.HasPrefix(strings.ToUpper(stmt), "DELETE "):
		return e.executeDelete(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "ROLLBACK "):
		return e.executeRollback(stmt)
	case strings.HasPrefix(strings.ToUpper(stmt), "CREATE RULE "):
		return e.executeCreateRule(stmt)
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
	value, eventName, err := parseSetPayload(parts[2])
	if err != nil {
		return nil, err
	}
	if eventName != "" {
		return e.engine.SetWithEventName(parts[1], value, eventName)
	}
	return e.engine.Set(parts[1], value)
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
		value, eventName, err := parseSetPayload(parts[1])
		if err != nil {
			return nil, err
		}
		items = append(items, db.BatchSetItem{
			Key:       parts[0],
			Value:     value,
			EventName: eventName,
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

func (e *Executor) executeDelete(stmt string) (any, error) {
	parts := strings.Fields(stmt)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid DELETE statement")
	}
	return e.engine.Delete(parts[1])
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

func parseSetPayload(input string) (json.RawMessage, string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, "", fmt.Errorf("missing SET value")
	}

	if idx := strings.LastIndex(trimmed, " "); idx > 0 {
		valuePart := strings.TrimSpace(trimmed[:idx])
		eventPart := strings.TrimSpace(trimmed[idx+1:])
		if isEventName(eventPart) {
			value, err := parseJSONOrScalar(valuePart)
			if err == nil {
				return value, eventPart, nil
			}
		}
	}

	value, err := parseJSONOrScalar(trimmed)
	return value, "", err
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
