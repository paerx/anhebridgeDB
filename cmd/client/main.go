package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

type request struct {
	Query string `json:"query"`
}

type response struct {
	Results []any `json:"results"`
}

type editor struct {
	fd       int
	original syscall.Termios
	raw      syscall.Termios
	history  []string
	width    int
	prevRows int
}

const (
	keyNull = iota
	keyCtrlC
	keyCtrlD
	keyEnter
	keyBackspace
	keyTab
	keyUp
	keyDown
	keyLeft
	keyRight
	keyRune
)

type keyEvent struct {
	kind int
	r    rune
}

var completions = []string{
	"SET",
	"GET",
	"DELETE",
	"CREATE RULE",
	"SHOW RULES",
	"SHOW RULE",
	"RUN SCHEDULER",
	"help",
	"exit",
	"quit",
}

func main() {
	var (
		addr    = flag.String("addr", "http://127.0.0.1:8080", "server address")
		execute = flag.String("e", "", "execute DSL and exit")
		timeout = flag.Duration("timeout", 10*time.Second, "request timeout")
	)
	flag.Parse()

	client := &http.Client{Timeout: *timeout}

	switch {
	case strings.TrimSpace(*execute) != "":
		if err := runOnce(client, *addr, *execute); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case stdinHasData():
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := runOnce(client, *addr, string(input)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		if err := repl(client, *addr); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func repl(client *http.Client, addr string) error {
	fmt.Printf("anhebridgedb-cli connected to %s\n", addr)
	printHelpSummary()

	ed, err := newEditor(int(os.Stdin.Fd()))
	if err != nil {
		return err
	}
	defer ed.close()

	var statement strings.Builder
	for {
		prompt := "anhe> "
		if statement.Len() > 0 {
			prompt = "....> "
		}

		line, err := ed.readLine(prompt)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println()
				return nil
			}
			return err
		}

		trimmed := strings.TrimSpace(line)
		if statement.Len() == 0 {
			switch trimmed {
			case "":
				continue
			case "help", "\\help":
				ed.withCooked(printHelpTable)
				continue
			case "exit", "quit":
				if confirmExit(ed, statement.String()) {
					fmt.Println("bye")
					return nil
				}
				continue
			}
		}

		statement.WriteString(line)
		statement.WriteByte('\n')
		if !strings.Contains(line, ";") {
			continue
		}

		ed.withCooked(func() {
			if err := runOnce(client, addr, statement.String()); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		})
		statement.Reset()
	}
}

func runOnce(client *http.Client, addr, query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	body, err := json.Marshal(request{Query: query})
	if err != nil {
		return err
	}

	resp, err := client.Post(strings.TrimRight(addr, "/")+"/dsl", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("server error (%s): %s", resp.Status, strings.TrimSpace(string(respBytes)))
	}

	var payload response
	if err := json.Unmarshal(respBytes, &payload); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	formatted, err := json.MarshalIndent(payload.Results, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(formatted))
	return nil
}

func newEditor(fd int) (*editor, error) {
	termios, err := getTermios(fd)
	if err != nil {
		return nil, err
	}

	raw := *termios
	raw.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK | syscall.ISTRIP | syscall.INLCR | syscall.IGNCR | syscall.ICRNL | syscall.IXON
	raw.Oflag &^= syscall.OPOST
	raw.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ICANON | syscall.ISIG | syscall.IEXTEN
	raw.Cflag &^= syscall.CSIZE | syscall.PARENB
	raw.Cflag |= syscall.CS8
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0

	if err := setTermios(fd, &raw); err != nil {
		return nil, err
	}

	return &editor{fd: fd, original: *termios, raw: raw}, nil
}

func (e *editor) close() error {
	return setTermios(e.fd, &e.original)
}

func (e *editor) enableRaw() error {
	return setTermios(e.fd, &e.raw)
}

func (e *editor) withCooked(fn func()) {
	_ = e.close()
	defer func() { _ = e.enableRaw() }()
	fn()
}

func (e *editor) readLine(prompt string) (string, error) {
	line := []rune{}
	cursor := 0
	historyIndex := len(e.history)
	e.width = terminalWidth(e.fd)
	e.prevRows = 1

	fmt.Print(prompt)
	for {
		key, err := readKey(e.fd)
		if err != nil {
			return "", err
		}

		switch key.kind {
		case keyCtrlC:
			fmt.Print("^C\n")
			return "", nil
		case keyCtrlD:
			if len(line) == 0 {
				return "", io.EOF
			}
		case keyEnter:
			fmt.Print("\r\n")
			e.prevRows = 0
			out := strings.TrimRight(string(line), "\r\n")
			if strings.TrimSpace(out) != "" {
				e.pushHistory(out)
			}
			return out, nil
		case keyBackspace:
			if cursor > 0 {
				line = append(line[:cursor-1], line[cursor:]...)
				cursor--
			}
		case keyLeft:
			if cursor > 0 {
				cursor--
			}
		case keyRight:
			if cursor < len(line) {
				cursor++
			}
		case keyUp:
			if len(e.history) > 0 && historyIndex > 0 {
				historyIndex--
				line = []rune(e.history[historyIndex])
				cursor = len(line)
			}
		case keyDown:
			if historyIndex < len(e.history)-1 {
				historyIndex++
				line = []rune(e.history[historyIndex])
				cursor = len(line)
			} else if historyIndex == len(e.history)-1 {
				historyIndex = len(e.history)
				line = []rune{}
				cursor = 0
			}
		case keyTab:
			suggestions := suggest(string(line))
			switch len(suggestions) {
			case 0:
			case 1:
				line = []rune(suggestions[0])
				cursor = len(line)
			default:
				e.withCooked(func() {
					fmt.Println()
					printSuggestionsTable(suggestions)
				})
			}
		case keyRune:
			line = append(line[:cursor], append([]rune{key.r}, line[cursor:]...)...)
			cursor++
		}

		e.redraw(prompt, line, cursor)
	}
}

func (e *editor) pushHistory(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	if len(e.history) > 0 && e.history[len(e.history)-1] == line {
		return
	}
	e.history = append(e.history, line)
}

func (e *editor) redraw(prompt string, line []rune, cursor int) {
	rows := visualRows(e.width, utf8Len(prompt)+len(line))
	clearRows(e.prevRows)
	e.prevRows = rows

	fmt.Print(prompt)
	fmt.Print(string(line))
	if cursor < len(line) {
		back := len(line) - cursor
		if back > 0 {
			fmt.Printf("\033[%dD", back)
		}
	}
}

func readKey(fd int) (keyEvent, error) {
	var buf [3]byte
	n, err := syscall.Read(fd, buf[:1])
	if err != nil {
		return keyEvent{}, err
	}
	if n == 0 {
		return keyEvent{}, io.EOF
	}

	switch buf[0] {
	case 3:
		return keyEvent{kind: keyCtrlC}, nil
	case 4:
		return keyEvent{kind: keyCtrlD}, nil
	case '\r', '\n':
		return keyEvent{kind: keyEnter}, nil
	case 127, 8:
		return keyEvent{kind: keyBackspace}, nil
	case '\t':
		return keyEvent{kind: keyTab}, nil
	case 27:
		if _, err := syscall.Read(fd, buf[1:3]); err != nil {
			return keyEvent{kind: keyNull}, nil
		}
		if buf[1] == '[' {
			switch buf[2] {
			case 'A':
				return keyEvent{kind: keyUp}, nil
			case 'B':
				return keyEvent{kind: keyDown}, nil
			case 'C':
				return keyEvent{kind: keyRight}, nil
			case 'D':
				return keyEvent{kind: keyLeft}, nil
			}
		}
		return keyEvent{kind: keyNull}, nil
	default:
		return keyEvent{kind: keyRune, r: rune(buf[0])}, nil
	}
}

func suggest(input string) []string {
	current := strings.TrimSpace(input)
	if current == "" {
		return append([]string(nil), completions...)
	}

	currentUpper := strings.ToUpper(current)
	var out []string
	for _, option := range completions {
		if strings.HasPrefix(strings.ToUpper(option), currentUpper) {
			out = append(out, option)
		}
	}
	sort.Strings(out)
	return out
}

func printHelpSummary() {
	fmt.Println("Keys: Up/Down history, Tab completion, help, exit")
}

func printHelpTable() {
	rows := [][2]string{
		{"SET key value;", "write a value"},
		{"GET key;", "read current value"},
		{"GET key AT 'ts';", "read value at RFC3339 time"},
		{"GET key ALLTIME;", "show full timeline"},
		{"GET key ALLTIME WITH DIFF;", "show timeline with field diff"},
		{"CREATE RULE ...;", "create auto transition rule"},
		{"SHOW RULES;", "list rules"},
		{"RUN SCHEDULER;", "process due tasks now"},
		{"exit / quit", "safe exit"},
	}
	printTable("Command", "Description", rows)
}

func printSuggestionsTable(items []string) {
	rows := make([][2]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, [2]string{item, "completion"})
	}
	printTable("Suggestion", "Type", rows)
}

func printTable(left, right string, rows [][2]string) {
	width := len(left)
	for _, row := range rows {
		if len(row[0]) > width {
			width = len(row[0])
		}
	}

	fmt.Printf("%-*s  %s\n", width, left, right)
	fmt.Printf("%s  %s\n", strings.Repeat("-", width), strings.Repeat("-", len(right)))
	for _, row := range rows {
		fmt.Printf("%-*s  %s\n", width, row[0], row[1])
	}
}

func confirmExit(ed *editor, pending string) bool {
	if strings.TrimSpace(pending) == "" {
		return true
	}
	_ = ed.close()
	defer func() { _ = ed.enableRaw() }()
	fmt.Print("Pending input will be discarded. Exit? [y/N]: ")
	var answer string
	fmt.Scanln(&answer)
	answer = strings.TrimSpace(strings.ToLower(answer))
	return answer == "y" || answer == "yes"
}

func stdinHasData() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice == 0
}

func clearRows(rows int) {
	if rows <= 0 {
		return
	}
	for i := 0; i < rows; i++ {
		fmt.Print("\r\033[2K")
		if i < rows-1 {
			fmt.Print("\033[1A")
		}
	}
	fmt.Print("\r")
}

func visualRows(width, content int) int {
	if width <= 0 {
		width = 80
	}
	rows := content / width
	if content%width != 0 {
		rows++
	}
	if rows == 0 {
		return 1
	}
	return rows
}

func utf8Len(s string) int {
	return len([]rune(s))
}

func terminalWidth(fd int) int {
	type winsize struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}
	ws := &winsize{}
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(ws)), 0, 0, 0)
	if errno != 0 || ws.Col == 0 {
		return 80
	}
	return int(ws.Col)
}

func getTermios(fd int) (*syscall.Termios, error) {
	termios := &syscall.Termios{}
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TIOCGETA), uintptr(unsafe.Pointer(termios)), 0, 0, 0)
	if errno != 0 {
		return nil, errno
	}
	return termios, nil
}

func setTermios(fd int, termios *syscall.Termios) error {
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TIOCSETA), uintptr(unsafe.Pointer(termios)), 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
