package backup

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

func (m *Manager) monitorLoop(ctx context.Context) {
	interval := time.Duration(m.cfg.Monitor.IntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.inspectMetrics()
		}
	}
}

func (m *Manager) inspectMetrics() {
	metrics := m.engine.Metrics()
	checks := map[string]bool{
		"heap_alloc_bytes":  exceeds(metrics["heap_alloc_bytes"], float64(m.cfg.Monitor.HeapAllocMaxBytes)),
		"pending_tasks":     exceeds(metrics["pending_tasks"], float64(m.cfg.Monitor.PendingTasksMax)),
		"overdue_tasks":     exceeds(metrics["overdue_tasks"], float64(m.cfg.Monitor.OverdueTasksMax)),
		"write_queue_depth": exceeds(metrics["write_queue_depth"], float64(m.cfg.Monitor.WriteQueueMax)),
		"append_p95_ms":     exceeds(metrics["append_p95_ms"], m.cfg.Monitor.AppendP95MaxMS),
		"get_p95_ms":        exceeds(metrics["get_p95_ms"], m.cfg.Monitor.GetP95MaxMS),
	}

	now := time.Now().UTC()
	var alerts []string
	m.monitorMu.Lock()
	for metric, breached := range checks {
		if !breached {
			m.breachCounts[metric] = 0
			continue
		}
		m.breachCounts[metric]++
		if m.breachCounts[metric] < m.cfg.Monitor.ConsecutiveBreaches {
			continue
		}
		if now.Sub(m.lastAlertTimes[metric]) < time.Duration(m.cfg.Monitor.AlertCooldownSeconds)*time.Second {
			continue
		}
		m.lastAlertTimes[metric] = now
		alerts = append(alerts, fmt.Sprintf("%s=%v", metric, metrics[metric]))
	}
	m.monitorMu.Unlock()
	if len(alerts) == 0 {
		return
	}
	sort.Strings(alerts)
	m.engine.RecordMonitorAlert()
	m.notify("[AnheBridgeDB] monitor ALERT\n" + strings.Join(alerts, "\n"))
}

func (m *Manager) verifyLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.cfg.Monitor.VerifyIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			start := time.Now()
			report, err := m.engine.VerifyStorage()
			if err != nil {
				m.engine.RecordMonitorAlert()
				m.notify(fmt.Sprintf("[AnheBridgeDB] storage verify FAILED\nerror: %v", err))
				continue
			}
			if !report.OK {
				m.engine.RecordMonitorAlert()
				m.notify(fmt.Sprintf("[AnheBridgeDB] storage verify TAMPER ALERT\nissues: %d\nduration: %s", len(report.Issues), time.Since(start).Round(time.Millisecond)))
				continue
			}
			log.Printf("automatic storage verify completed: segments=%d duration=%s", len(report.Segments), time.Since(start).Round(time.Millisecond))
		}
	}
}

func exceeds(value any, threshold float64) bool {
	if threshold <= 0 {
		return false
	}
	return metricFloat(value) > threshold
}

func metricFloat(value any) float64 {
	switch typed := value.(type) {
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint64:
		return float64(typed)
	case float64:
		return typed
	case float32:
		return float64(typed)
	default:
		return 0
	}
}

func (m *Manager) notificationLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-m.notifyCh:
			notifyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			if err := m.notifier.send(notifyCtx, message); err != nil {
				log.Printf("lark notification failed: %v", err)
			}
			cancel()
		}
	}
}

func (m *Manager) notify(message string) {
	if m.notifier == nil {
		return
	}
	select {
	case m.notifyCh <- message:
	default:
		log.Printf("lark notification dropped: queue full")
	}
}
