import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm';

// Global state
let db = null;
let conn = null;
let currentFileName = null;

// UI Elements
const statusEl = document.getElementById('status');
const statusText = document.getElementById('status-text');
const sampleSelect = document.getElementById('sample-select');
const loadSampleBtn = document.getElementById('load-sample');
const fileUpload = document.getElementById('file-upload');
const loadedFileEl = document.getElementById('loaded-file');
const loadedFileNameEl = document.getElementById('loaded-file-name');
const sqlEditor = document.getElementById('sql-editor');
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
        const bundle = await duckdb.selectBundle({
            mvp: {
                mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/dist/duckdb-mvp.wasm',
                mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/dist/duckdb-browser-mvp.worker.js',
            },
            eh: {
                mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/dist/duckdb-eh.wasm',
                mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/dist/duckdb-browser-eh.worker.js',
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

            showStatus('Ready! Load a JSON file to get started.', 'success');
        } catch (extErr) {
            console.error('Extension loading error:', extErr);
            showStatus(`Extension failed to load: ${extErr.message}`, 'error');
            return;
        }

        // Enable UI
        enableUI();

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

// Enable UI elements
function enableUI() {
    sampleSelect.disabled = false;
    loadSampleBtn.disabled = false;
    fileUpload.disabled = false;
    sqlEditor.disabled = false;
    document.querySelectorAll('.example-btn').forEach(btn => btn.disabled = false);
}

// Load sample file
async function loadSample() {
    const samplePath = sampleSelect.value;
    if (!samplePath) return;

    try {
        showStatus('Loading sample file...', 'loading');

        const response = await fetch(samplePath);
        if (!response.ok) throw new Error(`Failed to fetch ${samplePath}`);

        const arrayBuffer = await response.arrayBuffer();
        const fileName = samplePath.split('/').pop();

        await loadDataIntoDatabase(arrayBuffer, fileName);

        currentFileName = fileName;
        loadedFileNameEl.textContent = fileName;
        loadedFileEl.classList.remove('hidden');
        runQueryBtn.disabled = false;

        showStatus(`Loaded ${fileName}`, 'success');

    } catch (err) {
        console.error('Error loading sample:', err);
        showStatus(`Error loading sample: ${err.message}`, 'error');
    }
}

// Load uploaded file
async function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    try {
        showStatus(`Loading ${file.name}...`, 'loading');

        const arrayBuffer = await file.arrayBuffer();
        await loadDataIntoDatabase(arrayBuffer, file.name);

        currentFileName = file.name;
        loadedFileNameEl.textContent = file.name;
        loadedFileEl.classList.remove('hidden');
        runQueryBtn.disabled = false;

        showStatus(`Loaded ${file.name}`, 'success');

    } catch (err) {
        console.error('Error loading file:', err);
        showStatus(`Error loading file: ${err.message}`, 'error');
    }
}

// Load data into DuckDB's virtual filesystem
async function loadDataIntoDatabase(arrayBuffer, fileName) {
    const uint8Array = new Uint8Array(arrayBuffer);

    // Register file in DuckDB's virtual filesystem
    await db.registerFileBuffer('data', uint8Array);
}

// Run SQL query
async function runQuery() {
    const query = sqlEditor.value.trim();
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
    sqlEditor.value = query;
}

// Event listeners
loadSampleBtn.addEventListener('click', loadSample);
fileUpload.addEventListener('change', handleFileUpload);
runQueryBtn.addEventListener('click', runQuery);

exampleBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        const query = btn.getAttribute('data-query');
        setExampleQuery(query);
    });
});

// Allow Ctrl+Enter / Cmd+Enter to run query
sqlEditor.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        runQuery();
    }
});

// Initialize on page load
window.addEventListener('DOMContentLoaded', initDuckDB);
