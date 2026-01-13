import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm';

// Global state
let db = null;
let conn = null;
let currentFileName = null;
let sqlEditor = null; // Ace editor instance

// UI Elements
const statusEl = document.getElementById('status');
const statusText = document.getElementById('status-text');
const fileUpload = document.getElementById('file-upload');
const loadedFileEl = document.getElementById('loaded-file');
const loadedFileNameEl = document.getElementById('loaded-file-name');
const sqlEditorEl = document.getElementById('sql-editor');
const runQueryBtn = document.getElementById('run-query');
const exampleBtns = document.querySelectorAll('.example-btn');
const resultsPlaceholder = document.getElementById('results-placeholder');
const resultsTableContainer = document.getElementById('results-table-container');
const resultsTable = document.getElementById('results-table');
const resultsInfo = document.getElementById('results-info');
const resultsError = document.getElementById('results-error');

// Initialize DuckDB
async function initDuckDB() {
    try {
        showStatus('Initializing DuckDB WASM...', 'loading');

        // Select bundle based on browser capabilities
        // Note: Extension built with Emscripten 3.1.73, duckdb-wasm 1.32.0 uses 3.1.71
        const bundle = await duckdb.selectBundle({
            mvp: {
                mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-mvp.wasm',
                mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-mvp.worker.js',
            },
            eh: {
                mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-eh.wasm',
                mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-eh.worker.js',
            },
        });

        const worker_url = URL.createObjectURL(
            new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
        );
        const worker = new Worker(worker_url);
        const logger = new duckdb.ConsoleLogger();
        db = new duckdb.AsyncDuckDB(logger, worker);

        // Instantiate with configuration to allow unsigned extensions
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        URL.revokeObjectURL(worker_url);

        // Configure database to allow unsigned extensions
        await db.open({
            query: {
                castBigIntToDouble: true,
            },
            allowUnsignedExtensions: true,
        });

        // Connect to database
        conn = await db.connect();

        showStatus('Loading OTLP extension...', 'loading');

        // Try to load the OTLP extension
        try {
            // Load extension directly from URL (DuckDB-Wasm supports loading from full URLs)
            const extensionUrl = `${window.location.origin}${window.location.pathname}otlp.duckdb_extension.wasm`;
            console.log('Loading extension from:', extensionUrl);

            await conn.query(`LOAD "${extensionUrl}";`);

            showStatus('Loading sample data...', 'loading');
        } catch (extErr) {
            console.error('Extension loading error:', extErr);
            showStatus(`Extension failed to load: ${extErr.message}`, 'error');
            return;
        }

        // Preload sample files
        await preloadSampleFiles();

        // Enable UI
        await enableUI();

        showStatus('Ready! Sample data loaded - try the example queries below.', 'success');

    } catch (err) {
        console.error('DuckDB initialization error:', err);
        showStatus(`Failed to initialize: ${err.message}`, 'error');
    }
}

// Show status message
function showStatus(message, type = 'loading') {
    statusEl.className = `status ${type}`;
    statusText.textContent = message;
    statusEl.classList.remove('hidden');

    if (type === 'success') {
        setTimeout(() => statusEl.classList.add('hidden'), 3000);
    }
}

// Preload sample files into DuckDB
async function preloadSampleFiles() {
    const sampleFiles = [
        { path: 'samples/traces_simple.jsonl', name: 'traces.jsonl' },
        { path: 'samples/logs_simple.jsonl', name: 'logs.jsonl' },
        { path: 'samples/metrics_simple.jsonl', name: 'metrics.jsonl' }
    ];

    try {
        for (const file of sampleFiles) {
            const response = await fetch(file.path);
            if (!response.ok) {
                console.warn(`Failed to load ${file.path}`);
                continue;
            }
            const arrayBuffer = await response.arrayBuffer();
            const uint8Array = new Uint8Array(arrayBuffer);
            await db.registerFileBuffer(file.name, uint8Array);
            console.log(`Registered ${file.name}`);
        }
    } catch (err) {
        console.error('Error preloading samples:', err);
        // Don't fail the whole app if samples fail to load
    }
}

// Initialize Ace SQL editor
async function initSQLEditor() {
    // Wait for Ace to be available
    while (!window.ace) {
        await new Promise(resolve => setTimeout(resolve, 50));
    }

    sqlEditor = ace.edit(sqlEditorEl, {
        mode: "ace/mode/sql",
        theme: "ace/theme/chrome",
        value: "SELECT * FROM read_otlp_traces('traces.jsonl') LIMIT 10;",
        minLines: 8,
        maxLines: 20,
        fontSize: 14,
        showPrintMargin: false,
        highlightActiveLine: true,
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: false,
        wrap: true
    });

    // Add Ctrl/Cmd+Enter keyboard shortcut to run query
    sqlEditor.commands.addCommand({
        name: 'runQuery',
        bindKey: { win: 'Ctrl-Enter', mac: 'Command-Enter' },
        exec: function(editor) {
            runQuery();
        }
    });
}

// Enable UI elements
async function enableUI() {
    fileUpload.disabled = false;
    runQueryBtn.disabled = false; // Enable immediately since samples are preloaded
    document.querySelectorAll('.example-btn').forEach(btn => btn.disabled = false);

    // Initialize SQL editor (wait for CodeMirror to load)
    await initSQLEditor();
}

// Load uploaded file
async function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    try {
        showStatus(`Loading ${file.name}...`, 'loading');

        const arrayBuffer = await file.arrayBuffer();
        const uint8Array = new Uint8Array(arrayBuffer);

        // Register file as 'userdata' in DuckDB's virtual filesystem
        await db.registerFileBuffer('userdata', uint8Array);

        currentFileName = file.name;
        loadedFileNameEl.textContent = `${file.name} (available as 'userdata')`;
        loadedFileEl.classList.remove('hidden');

        showStatus(`Loaded ${file.name} - query it with read_otlp_*('userdata')`, 'success');

    } catch (err) {
        console.error('Error loading file:', err);
        showStatus(`Error loading file: ${err.message}`, 'error');
    }
}

// Run SQL query
async function runQuery() {
    const query = sqlEditor.getValue().trim();
    if (!query) {
        showStatus('Please enter a SQL query', 'error');
        return;
    }

    try {
        showStatus('Running query...', 'loading');
        hideError();

        const startTime = performance.now();
        const result = await conn.query(query);
        const endTime = performance.now();

        displayResults(result, endTime - startTime);
        showStatus('Query completed successfully', 'success');

    } catch (err) {
        console.error('Query error:', err);
        showError(err.message);
        showStatus('Query failed', 'error');
    }
}

// Display query results in table
function displayResults(result, executionTime) {
    resultsPlaceholder.classList.add('hidden');
    resultsTableContainer.classList.remove('hidden');

    // Show info
    const rowCount = result.numRows;
    const colCount = result.numCols;
    resultsInfo.textContent = `${rowCount} row${rowCount !== 1 ? 's' : ''}, ${colCount} column${colCount !== 1 ? 's' : ''} (${executionTime.toFixed(2)}ms)`;

    // Get column names
    const columns = result.schema.fields.map(f => f.name);

    // Build table header
    const thead = resultsTable.querySelector('thead');
    thead.innerHTML = '<tr>' + columns.map(col => `<th>${escapeHtml(col)}</th>`).join('') + '</tr>';

    // Build table body
    const tbody = resultsTable.querySelector('tbody');
    tbody.innerHTML = '';

    // Convert result to array of rows
    const rows = result.toArray();

    rows.forEach(row => {
        const tr = document.createElement('tr');
        columns.forEach(col => {
            const td = document.createElement('td');
            const value = row[col];
            td.textContent = formatValue(value);
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
}

// Format cell value
function formatValue(value) {
    if (value === null || value === undefined) {
        return 'NULL';
    }
    if (typeof value === 'object') {
        return JSON.stringify(value);
    }
    return String(value);
}

// Show error message
function showError(message) {
    resultsError.textContent = message;
    resultsError.classList.remove('hidden');
    resultsPlaceholder.classList.add('hidden');
    resultsTableContainer.classList.add('hidden');
}

// Hide error message
function hideError() {
    resultsError.classList.add('hidden');
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Set example query in editor
function setExampleQuery(query) {
    if (sqlEditor) {
        sqlEditor.setValue(query, -1); // -1 moves cursor to end
    }
}

// Event listeners
fileUpload.addEventListener('change', handleFileUpload);
runQueryBtn.addEventListener('click', runQuery);

exampleBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        const query = btn.getAttribute('data-query');
        setExampleQuery(query);
    });
});

// Initialize on page load
window.addEventListener('DOMContentLoaded', initDuckDB);
