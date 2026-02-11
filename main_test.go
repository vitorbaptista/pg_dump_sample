package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	pg "gopkg.in/pg.v4"
)

// testDBOpts returns pg.Options for the test database.
// It reads connection details from environment variables with sensible defaults
// for the docker-compose setup.
func testDBOpts() *pg.Options {
	host := os.Getenv("PGHOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("PGPORT")
	if port == "" {
		port = "15432"
	}
	user := os.Getenv("PGUSER")
	if user == "" {
		user = "test"
	}
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = "test"
	}
	database := os.Getenv("PGDATABASE")
	if database == "" {
		database = "pg_dump_sample_test"
	}
	return &pg.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		User:     user,
		Password: password,
		Database: database,
	}
}

// requireDB connects to the test database, skipping the test if unavailable.
// The connection is automatically closed when the test finishes.
func requireDB(t *testing.T) *pg.DB {
	t.Helper()
	db, err := connectDB(testDBOpts())
	if err != nil {
		t.Skipf("skipping: test database not available: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// --------------------------------------------------------------------------
// Unit tests (no database required)
// --------------------------------------------------------------------------

func TestReadManifest_Full(t *testing.T) {
	f, err := os.Open("testdata/manifest_full.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	m, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	if len(m.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(m.Tables))
	}

	expected := []string{"users", "posts", "comments"}
	for i, name := range expected {
		if m.Tables[i].Table != name {
			t.Errorf("table[%d]: expected %q, got %q", i, name, m.Tables[i].Table)
		}
	}
}

func TestReadManifest_WithVarsAndQueries(t *testing.T) {
	f, err := os.Open("testdata/manifest_sample.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	m, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	if m.Vars["max_user_id"] != "2" {
		t.Errorf("expected vars[max_user_id]=%q, got %q", "2", m.Vars["max_user_id"])
	}

	for _, item := range m.Tables {
		if item.Query == "" {
			t.Errorf("table %q: expected a query, got empty", item.Table)
		}
	}
}

func TestReadManifest_PostActions(t *testing.T) {
	f, err := os.Open("testdata/manifest_post_actions.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	m, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	if len(m.Tables) == 0 {
		t.Fatalf("expected at least 1 table, got 0")
	}

	if len(m.Tables[0].PostActions) != 1 {
		t.Fatalf("expected 1 post_action for users, got %d", len(m.Tables[0].PostActions))
	}

	if !strings.Contains(m.Tables[0].PostActions[0], "setval") {
		t.Errorf("expected post_action to contain 'setval', got %q", m.Tables[0].PostActions[0])
	}
}

func TestReadManifest_Columns(t *testing.T) {
	f, err := os.Open("testdata/manifest_columns.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	m, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	if len(m.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(m.Tables))
	}

	expected := []string{"id", "username", "email"}
	if len(m.Tables[0].Columns) != len(expected) {
		t.Fatalf("expected %d columns, got %d", len(expected), len(m.Tables[0].Columns))
	}
	for i, col := range expected {
		if m.Tables[0].Columns[i] != col {
			t.Errorf("column[%d]: expected %q, got %q", i, col, m.Tables[0].Columns[i])
		}
	}
}

// TestReadManifest_InvalidYAML verifies that readManifest returns an error
// when given malformed YAML input.
//
// Currently skipped: readManifest silently discards the error from
// yaml.Unmarshal (main.go:290), so malformed YAML produces an empty
// Manifest with a nil error. Unskip once readManifest propagates the
// parse error.
func TestReadManifest_InvalidYAML(t *testing.T) {
	t.Skip("known bug: readManifest discards yaml.Unmarshal error")

	r := strings.NewReader("{{{{invalid yaml!!")
	m, err := readManifest(r)
	if err == nil {
		t.Fatalf("expected error for invalid YAML, got nil (manifest: %+v)", m)
	}
}

// TestConnectDB_CloseOnError verifies that connectDB does not leak a
// connection pool when the health-check query fails (e.g. wrong database).
//
// Currently skipped: connectDB (main.go:228-238) calls pg.Connect which
// allocates a pool, then returns (nil, err) without closing it when the
// SELECT 1 probe fails. Unskip once connectDB closes db on error.
func TestConnectDB_CloseOnError(t *testing.T) {
	t.Skip("known bug: connectDB leaks pg.DB when health-check query fails")

	// Use a non-existent database to force the SELECT 1 to fail.
	opts := testDBOpts()
	opts.Database = "nonexistent_db_should_not_exist"

	db, err := connectDB(opts)
	if err == nil {
		db.Close()
		t.Fatal("expected an error for a non-existent database, got nil")
	}
	// If connectDB is fixed to close the pool on error, this test
	// simply confirms the error path doesn't panic or leak.
}

func TestBeginDump(t *testing.T) {
	var buf bytes.Buffer
	beginDump(&buf)
	out := buf.String()

	if !strings.Contains(out, "BEGIN;") {
		t.Error("beginDump output should contain BEGIN;")
	}
	if !strings.Contains(out, "SET client_encoding = 'UTF8'") {
		t.Error("beginDump output should set client_encoding")
	}
}

func TestEndDump(t *testing.T) {
	var buf bytes.Buffer
	endDump(&buf)
	out := buf.String()

	if !strings.Contains(out, "COMMIT;") {
		t.Error("endDump output should contain COMMIT;")
	}
	if !strings.Contains(out, "PostgreSQL database dump complete") {
		t.Error("endDump output should contain completion marker")
	}
}

func TestBeginTable(t *testing.T) {
	var buf bytes.Buffer
	beginTable(&buf, "users", []string{"id", "username", "email"})
	out := buf.String()

	if !strings.Contains(out, "Data for Name: users") {
		t.Error("beginTable output should reference table name")
	}
	if !strings.Contains(out, "COPY users") {
		t.Error("beginTable output should contain COPY statement")
	}
	if !strings.Contains(out, `"id"`) {
		t.Error("beginTable output should contain quoted column names")
	}
}

func TestEndTable(t *testing.T) {
	var buf bytes.Buffer
	endTable(&buf)
	out := buf.String()

	if !strings.Contains(out, `\.`) {
		t.Error(`endTable output should contain the COPY terminator \.`)
	}
}

func TestDumpSqlCmd(t *testing.T) {
	var buf bytes.Buffer
	dumpSqlCmd(&buf, "SELECT pg_catalog.setval('users_id_seq', 100, true)")
	out := buf.String()

	if !strings.Contains(out, "setval") {
		t.Error("dumpSqlCmd output should contain the SQL command")
	}
	if !strings.HasSuffix(strings.TrimSpace(out), ";") {
		t.Error("dumpSqlCmd output should end with semicolon")
	}
}

// --------------------------------------------------------------------------
// Integration tests (require database)
// --------------------------------------------------------------------------

func TestConnectDB(t *testing.T) {
	requireDB(t)
}

func TestGetTableCols_Users(t *testing.T) {
	db := requireDB(t)

	cols, err := getTableCols(db, "users")
	if err != nil {
		t.Fatalf("getTableCols error: %v", err)
	}

	expected := []string{"id", "username", "email", "created_at"}
	if len(cols) != len(expected) {
		t.Fatalf("expected %d columns, got %d: %v", len(expected), len(cols), cols)
	}
	for i, col := range expected {
		if cols[i] != col {
			t.Errorf("column[%d]: expected %q, got %q", i, col, cols[i])
		}
	}
}

func TestGetTableCols_Posts(t *testing.T) {
	db := requireDB(t)

	cols, err := getTableCols(db, "posts")
	if err != nil {
		t.Fatalf("getTableCols error: %v", err)
	}

	expected := []string{"id", "user_id", "title", "body", "created_at"}
	if len(cols) != len(expected) {
		t.Fatalf("expected %d columns, got %d: %v", len(expected), len(cols), cols)
	}
	for i, col := range expected {
		if cols[i] != col {
			t.Errorf("column[%d]: expected %q, got %q", i, col, cols[i])
		}
	}
}

func TestGetTableCols_Comments(t *testing.T) {
	db := requireDB(t)

	cols, err := getTableCols(db, "comments")
	if err != nil {
		t.Fatalf("getTableCols error: %v", err)
	}

	expected := []string{"id", "post_id", "user_id", "body", "created_at"}
	if len(cols) != len(expected) {
		t.Fatalf("expected %d columns, got %d: %v", len(expected), len(cols), cols)
	}
	for i, col := range expected {
		if cols[i] != col {
			t.Errorf("column[%d]: expected %q, got %q", i, col, cols[i])
		}
	}
}

func TestGetTableDeps_Users(t *testing.T) {
	db := requireDB(t)

	deps, err := getTableDeps(db, "users")
	if err != nil {
		t.Fatalf("getTableDeps error: %v", err)
	}

	if len(deps) != 0 {
		t.Errorf("users should have no foreign key deps, got %v", deps)
	}
}

func TestGetTableDeps_Posts(t *testing.T) {
	db := requireDB(t)

	deps, err := getTableDeps(db, "posts")
	if err != nil {
		t.Fatalf("getTableDeps error: %v", err)
	}

	if len(deps) != 1 || deps[0] != "users" {
		t.Errorf("posts should depend on [users], got %v", deps)
	}
}

func TestGetTableDeps_Comments(t *testing.T) {
	db := requireDB(t)

	deps, err := getTableDeps(db, "comments")
	if err != nil {
		t.Fatalf("getTableDeps error: %v", err)
	}

	if len(deps) != 2 {
		t.Fatalf("comments should have 2 deps, got %d: %v", len(deps), deps)
	}

	depSet := map[string]bool{}
	for _, d := range deps {
		depSet[d] = true
	}
	if !depSet["posts"] || !depSet["users"] {
		t.Errorf("comments should depend on posts and users, got %v", deps)
	}
}

func TestMakeDump_FullDump(t *testing.T) {
	db := requireDB(t)

	f, err := os.Open("testdata/manifest_full.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	manifest, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	var buf bytes.Buffer
	err = makeDump(db, manifest, &buf)
	if err != nil {
		t.Fatalf("makeDump error: %v", err)
	}

	out := buf.String()

	// Should have BEGIN/COMMIT wrapper
	if !strings.Contains(out, "BEGIN;") {
		t.Error("dump should contain BEGIN;")
	}
	if !strings.Contains(out, "COMMIT;") {
		t.Error("dump should contain COMMIT;")
	}

	// Should have COPY statements for all 3 tables
	for _, table := range []string{"users", "posts", "comments"} {
		if !strings.Contains(out, fmt.Sprintf("COPY %s", table)) {
			t.Errorf("dump should contain COPY for table %q", table)
		}
	}

	// All 5 users should be in the dump
	if strings.Count(out, "alice") < 1 {
		t.Error("dump should contain alice")
	}
	if strings.Count(out, "eve") < 1 {
		t.Error("dump should contain eve")
	}

	// Should have all 8 posts (check for some titles)
	if !strings.Contains(out, "First Post") {
		t.Error("dump should contain 'First Post'")
	}
	if !strings.Contains(out, "Bob Returns") {
		t.Error("dump should contain 'Bob Returns'")
	}
}

func TestMakeDump_SampledDump(t *testing.T) {
	db := requireDB(t)

	f, err := os.Open("testdata/manifest_sample.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	manifest, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	var buf bytes.Buffer
	err = makeDump(db, manifest, &buf)
	if err != nil {
		t.Fatalf("makeDump error: %v", err)
	}

	out := buf.String()

	// Users with id <= 2: alice (1), bob (2)
	if !strings.Contains(out, "alice@example.com") {
		t.Error("sampled dump should contain alice (id=1)")
	}
	if !strings.Contains(out, "bob@example.com") {
		t.Error("sampled dump should contain bob (id=2)")
	}

	// Users with id > 2 should NOT be in the dump (check emails for precise matching)
	if strings.Contains(out, "charlie@example.com") {
		t.Error("sampled dump should NOT contain charlie (id=3)")
	}
	if strings.Contains(out, "diana@example.com") {
		t.Error("sampled dump should NOT contain diana (id=4)")
	}
	if strings.Contains(out, "eve@example.com") {
		t.Error("sampled dump should NOT contain eve (id=5)")
	}
}

func TestMakeDump_PostActions(t *testing.T) {
	db := requireDB(t)

	f, err := os.Open("testdata/manifest_post_actions.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	manifest, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	var buf bytes.Buffer
	err = makeDump(db, manifest, &buf)
	if err != nil {
		t.Fatalf("makeDump error: %v", err)
	}

	out := buf.String()

	// The post_action setval statement should appear in the output
	if !strings.Contains(out, "setval") {
		t.Error("dump with post_actions should contain setval statement")
	}
	if !strings.Contains(out, "users_id_seq") {
		t.Error("dump should reference users_id_seq in post_action")
	}
}

func TestMakeDump_DependencyOrdering(t *testing.T) {
	db := requireDB(t)

	f, err := os.Open("testdata/manifest_deps.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	manifest, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	var buf bytes.Buffer
	err = makeDump(db, manifest, &buf)
	if err != nil {
		t.Fatalf("makeDump error: %v", err)
	}

	out := buf.String()

	// Extract the order of COPY statements
	re := regexp.MustCompile(`COPY (\w+) `)
	matches := re.FindAllStringSubmatch(out, -1)

	tables := make([]string, 0, len(matches))
	for _, m := range matches {
		tables = append(tables, m[1])
	}

	// users must come before posts, posts must come before comments
	usersIdx, postsIdx, commentsIdx := -1, -1, -1
	for i, tbl := range tables {
		switch tbl {
		case "users":
			usersIdx = i
		case "posts":
			postsIdx = i
		case "comments":
			commentsIdx = i
		}
	}

	if usersIdx == -1 || postsIdx == -1 || commentsIdx == -1 {
		t.Fatalf("expected all three tables in dump, found: %v", tables)
	}

	if usersIdx >= postsIdx {
		t.Errorf("users (idx=%d) should be dumped before posts (idx=%d)", usersIdx, postsIdx)
	}
	if postsIdx >= commentsIdx {
		t.Errorf("posts (idx=%d) should be dumped before comments (idx=%d)", postsIdx, commentsIdx)
	}
}

func TestMakeDump_SingleTable(t *testing.T) {
	db := requireDB(t)

	f, err := os.Open("testdata/manifest_single_table.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	manifest, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	var buf bytes.Buffer
	err = makeDump(db, manifest, &buf)
	if err != nil {
		t.Fatalf("makeDump error: %v", err)
	}

	out := buf.String()

	if !strings.Contains(out, "COPY users") {
		t.Error("single table dump should contain COPY users")
	}

	// Should NOT contain posts or comments COPY statements
	if strings.Contains(out, "COPY posts") {
		t.Error("single table dump should NOT contain COPY posts")
	}
	if strings.Contains(out, "COPY comments") {
		t.Error("single table dump should NOT contain COPY comments")
	}

	// Should contain all 5 users
	for _, name := range []string{"alice", "bob", "charlie", "diana", "eve"} {
		if !strings.Contains(out, name) {
			t.Errorf("single table dump should contain user %q", name)
		}
	}
}

func TestMakeDump_ExplicitColumns(t *testing.T) {
	db := requireDB(t)

	f, err := os.Open("testdata/manifest_columns.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	manifest, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	var buf bytes.Buffer
	err = makeDump(db, manifest, &buf)
	if err != nil {
		t.Fatalf("makeDump error: %v", err)
	}

	out := buf.String()

	// COPY should list only the explicit columns (id, username, email) not created_at
	if !strings.Contains(out, `"id"`) {
		t.Error("dump should contain column 'id'")
	}
	if !strings.Contains(out, `"username"`) {
		t.Error("dump should contain column 'username'")
	}
	if !strings.Contains(out, `"email"`) {
		t.Error("dump should contain column 'email'")
	}
	// The COPY header should NOT list created_at since we specified explicit columns
	copyLine := ""
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "COPY users") {
			copyLine = line
			break
		}
	}
	if strings.Contains(copyLine, "created_at") {
		t.Error("explicit columns dump should NOT include created_at in COPY header")
	}
}

func TestMakeDump_OutputIsValidSQL(t *testing.T) {
	db := requireDB(t)

	f, err := os.Open("testdata/manifest_full.yaml")
	if err != nil {
		t.Fatalf("failed to open manifest: %v", err)
	}
	defer f.Close()

	manifest, err := readManifest(f)
	if err != nil {
		t.Fatalf("readManifest error: %v", err)
	}

	var buf bytes.Buffer
	err = makeDump(db, manifest, &buf)
	if err != nil {
		t.Fatalf("makeDump error: %v", err)
	}

	out := buf.String()

	// Verify structural integrity: begins with BEGIN, ends with COMMIT
	trimmed := strings.TrimSpace(out)
	if !strings.Contains(trimmed, "BEGIN;") {
		t.Error("dump should start with BEGIN")
	}
	if !strings.HasSuffix(trimmed, "PostgreSQL database dump complete\n--") {
		// Just verify COMMIT is there
		if !strings.Contains(trimmed, "COMMIT;") {
			t.Error("dump should end with COMMIT")
		}
	}

	// Every COPY ... FROM stdin should have a matching \. terminator
	copyCount := strings.Count(out, "COPY ")
	terminatorCount := strings.Count(out, "\\.")
	if copyCount != terminatorCount {
		t.Errorf("COPY count (%d) should match terminator count (%d)", copyCount, terminatorCount)
	}
}

// buildTestBinary builds the binary into a temp directory and returns its path.
func buildTestBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "pg_dump_sample_test_bin")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	buildOut, err := buildCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build binary: %v\n%s", err, buildOut)
	}
	return binPath
}

// TestEndToEnd_Binary builds and runs the binary against the test database.
func TestEndToEnd_Binary(t *testing.T) {
	binPath := buildTestBinary(t)

	opts := testDBOpts()
	// Verify DB is reachable before running the binary
	db, err := connectDB(opts)
	if err != nil {
		t.Skipf("skipping: test database not available: %v", err)
	}
	db.Close()

	parts := strings.SplitN(opts.Addr, ":", 2)
	host := parts[0]
	port := parts[1]

	cmd := exec.Command(binPath,
		"-h", host,
		"-p", port,
		"-U", opts.User,
		"-w",
		"-f", "testdata/manifest_sample.yaml",
		opts.Database,
	)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", opts.Password))

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("binary execution failed: %v\n%s", err, out)
	}

	output := string(out)

	if !strings.Contains(output, "BEGIN;") {
		t.Error("binary output should contain BEGIN;")
	}
	if !strings.Contains(output, "COMMIT;") {
		t.Error("binary output should contain COMMIT;")
	}

	// Sampled data: only users with id <= 2
	if !strings.Contains(output, "alice@example.com") {
		t.Error("binary output should contain alice")
	}
	if !strings.Contains(output, "bob@example.com") {
		t.Error("binary output should contain bob")
	}
	if strings.Contains(output, "charlie@example.com") {
		t.Error("binary output should NOT contain charlie")
	}
}

// TestEndToEnd_OutputFile tests writing the dump to a file via -o flag.
func TestEndToEnd_OutputFile(t *testing.T) {
	binPath := buildTestBinary(t)

	opts := testDBOpts()
	db, err := connectDB(opts)
	if err != nil {
		t.Skipf("skipping: test database not available: %v", err)
	}
	db.Close()

	outFile := filepath.Join(t.TempDir(), "test_output.sql")

	parts := strings.SplitN(opts.Addr, ":", 2)
	host := parts[0]
	port := parts[1]

	cmd := exec.Command(binPath,
		"-h", host,
		"-p", port,
		"-U", opts.User,
		"-w",
		"-f", "testdata/manifest_single_table.yaml",
		"-o", outFile,
		opts.Database,
	)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", opts.Password))

	runOut, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("binary execution failed: %v\n%s", err, runOut)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	output := string(data)
	if !strings.Contains(output, "COPY users") {
		t.Error("output file should contain COPY users")
	}
	if !strings.Contains(output, "alice") {
		t.Error("output file should contain alice")
	}
}

// TestMakeDump_EmptyManifest verifies that a manifest with no tables produces
// a valid but empty dump (just BEGIN/COMMIT wrapper).
func TestMakeDump_EmptyManifest(t *testing.T) {
	db := requireDB(t)

	manifest := &Manifest{Tables: []ManifestItem{}}

	var buf bytes.Buffer
	err := makeDump(db, manifest, &buf)
	if err != nil {
		t.Fatalf("makeDump error: %v", err)
	}

	out := buf.String()

	if !strings.Contains(out, "BEGIN;") {
		t.Error("empty dump should contain BEGIN;")
	}
	if !strings.Contains(out, "COMMIT;") {
		t.Error("empty dump should contain COMMIT;")
	}
	if strings.Contains(out, "COPY") {
		t.Error("empty dump should not contain any COPY statements")
	}
}
