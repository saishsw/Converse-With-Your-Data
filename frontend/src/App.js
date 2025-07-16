import React, { useState, useEffect, useRef } from 'react';
import Plot from 'react-plotly.js';

// Helper function to convert string to Title Case
const toTitleCase = (str) => {
    if (!str) return '';
    return str.replace(
        /\w\S*/g,
        function(txt) {
            return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
        }
    );
};

// Main App component
const App = () => {
    // --- State Variables ---
    const [selectedFile, setSelectedFile] = useState(null);
    const [tableName, setTableName] = useState('');
    const [dataStartLine, setDataStartLine] = useState('');
    const [message, setMessage] = useState('');
    const [schema, setSchema] = useState(null);
    const [initialSuggestions, setInitialSuggestions] = useState([]);
    const [selectedVizTypes, setSelectedVizTypes] = useState([]);
    const [generatedPlots, setGeneratedPlots] = useState([]);
    const [userQuery, setUserQuery] = useState(''); // Manages chatbot input
    const [loading, setLoading] = useState(false);
    const [isDataIngested, setIsDataIngested] = useState(false);

    // Chat history and raw data
    const [chatHistory, setChatHistory] = useState([]);
    const [fullRawData, setFullRawData] = useState(null); // Stores the FULL raw data

    // Ref for auto-scrolling chat history
    const chatHistoryEndRef = useRef(null);

    // --- Backend URL ---
    const BACKEND_URL = 'http://localhost:8000';

    // Scroll to bottom of chat history when it updates
    useEffect(() => {
        if (chatHistoryEndRef.current) {
            chatHistoryEndRef.current.scrollIntoView({ behavior: 'smooth' });
        }
    }, [chatHistory]);

    // --- Handlers ---

    const handleFileChange = (event) => {
        if (event.target.files.length > 0) {
            setSelectedFile(event.target.files[0]);
            setMessage(`File selected: ${event.target.files[0].name}`);
            setSchema(null);
            setInitialSuggestions([]);
            setSelectedVizTypes([]);
            setGeneratedPlots([]);
            setFullRawData(null); // Clear full raw data
            setChatHistory([]);
            setIsDataIngested(false);
        } else {
            setSelectedFile(null);
            setMessage('No file selected.');
        }
    };

    const handleIngestAndSuggest = async () => {
        if (!selectedFile) {
            setMessage('Please upload a CSV file.');
            return;
        }
        if (!tableName.trim()) {
            setMessage('Please enter a table name.');
            return;
        }
        if (!dataStartLine.trim() || isNaN(parseInt(dataStartLine))) {
            setMessage('Please enter a valid number for data start line.');
            return;
        }

        setLoading(true);
        setMessage('Uploading file and generating initial suggestions...');
        const formData = new FormData();
        formData.append('file', selectedFile);
        formData.append('table_name', tableName);
        formData.append('data_start_line', dataStartLine);

        try {
            const response = await fetch(`${BACKEND_URL}/ingestion_suggestion/`, {
                method: 'POST',
                body: formData,
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to ingest CSV and get suggestions.');
            }

            const data = await response.json();
            setSchema(data.schema);
            setInitialSuggestions(data.initial_suggestions);
            setSelectedVizTypes(data.initial_suggestions.map(s => s.type));
            setFullRawData(data.initial_data_preview); // Store the FULL raw data
            setMessage(data.message);
            setGeneratedPlots([]);
            setChatHistory([]);
            setIsDataIngested(true);

        } catch (error) {
            setMessage(`Error: ${error.message}`);
            console.error('Ingestion/Suggestion Error:', error);
        } finally {
            setLoading(false);
        }
    };

    const handleVizTypeChange = (type) => {
        setSelectedVizTypes(prevSelected =>
            prevSelected.includes(type)
                ? prevSelected.filter(t => t !== type)
                : [...prevSelected, type]
        );
    };

    const handleGenerateVisualizations = async () => {
        if (selectedVizTypes.length === 0) {
            setMessage('Please select at least one visualization type.');
            return;
        }
        if (!schema) {
            setMessage('Error: Data not ingested. Please upload a CSV file first.');
            return;
        }

        setLoading(true);
        setMessage('Generating selected visualizations...');
        const formData = new FormData();
        formData.append('plot_types', selectedVizTypes.join(','));

        try {
            const response = await fetch(`${BACKEND_URL}/generate_visualization/`, {
                method: 'POST',
                body: formData,
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to generate visualizations.');
            }

            const data = await response.json();
            setGeneratedPlots(data.plots);
            setMessage(data.message);
            if (data.errors && data.errors.length > 0) {
                setMessage(prev => prev + " Some plots could not be generated: " + data.errors.join("; "));
            }
        } catch (error) {
            setMessage(`Error generating plots: ${error.message}`);
            console.error('Visualization Generation Error:', error);
        } finally {
            setLoading(false);
        }
    };

    const handleQueryData = async () => {
        if (!userQuery.trim()) {
            setMessage('Please enter a natural language query.');
            return;
        }
        if (!schema) {
            setMessage('Error: Data not ingested. Please upload a CSV file first.');
            return;
        }

        setLoading(true);
        setMessage('Processing natural language query...');
        
        const currentUserQuery = userQuery;
        setChatHistory(prev => [...prev, { type: 'user', content: currentUserQuery }]);
        setUserQuery('');

        const formData = new FormData();
        formData.append('user_query', currentUserQuery);

        try {
            const response = await fetch(`${BACKEND_URL}/query/`, {
                method: 'POST',
                body: formData,
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to query data.');
            }

            const data = await response.json();
            
            if (data.sql_query) {
                setChatHistory(prev => [...prev, { type: 'sql', content: data.sql_query }]);
            }

            if (data.results && Array.isArray(data.results) && data.results.length > 0) {
                setChatHistory(prev => [...prev, { type: 'result', content: data.results }]);
            } else {
                setChatHistory(prev => [...prev, { type: 'result', content: "Query executed successfully, but returned no results." }]);
            }
            
            setMessage(data.message);
            setGeneratedPlots([]);

        } catch (error) {
            setMessage(`Error querying data: ${error.message}`);
            console.error('Query Data Error:', error);
            setChatHistory(prev => [...prev, { type: 'error', content: `Error: ${error.message}` }]);
        } finally {
            setLoading(false);
        }
    };

    // --- New Handler for Downloading CSV ---
    const handleDownloadCsv = (data, filename = 'query_results.csv') => {
        if (!data || data.length === 0) {
            setMessage('No data to download.');
            return;
        }

        // Extract headers
        const headers = Object.keys(data[0]);
        // Create CSV rows
        const csvRows = [
            headers.join(','), // Header row
            ...data.map(row => headers.map(fieldName => {
                let value = row[fieldName];
                if (value === null || value === undefined) {
                    value = ''; // Handle null/undefined values
                } else if (typeof value === 'string') {
                    // Escape double quotes and wrap in double quotes if it contains commas or double quotes
                    value = `"${value.replace(/"/g, '""')}"`;
                }
                return value;
            }).join(','))
        ];

        const csvString = csvRows.join('\n');
        const blob = new Blob([csvString], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');

        // Create a URL for the blob and set download attributes
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', filename);
        link.style.visibility = 'hidden'; // Hide the link
        document.body.appendChild(link); // Append to body
        link.click(); // Programmatically click
        document.body.removeChild(link); // Clean up
        URL.revokeObjectURL(url); // Release the object URL
        setMessage('CSV downloaded successfully!');
    };


    // --- Components for new layout ---

    // Raw Data Table Component
    const RawDataTable = ({ data }) => {
        if (!data || data.length === 0) {
            return <div className="text-gray-500 text-center p-4">No raw data table available.</div>;
        }
        return (
            <div className="overflow-auto h-full">
                <table className="min-w-full divide-y divide-gray-200 text-left text-xs">
                    <thead className="bg-gray-100 sticky top-0 bg-gray-100 z-10">
                        <tr>
                            {Object.keys(data[0]).map((key) => (
                                <th key={key} className="px-3 py-2 font-medium text-gray-600 uppercase tracking-wider">
                                    {key}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                        {data.map((row, rowIndex) => (
                            <tr key={rowIndex}>
                                {Object.values(row).map((value, colIndex) => (
                                    <td key={colIndex} className="px-3 py-2 whitespace-nowrap text-gray-900">
                                        {String(value)}
                                    </td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        );
    };


    // --- Conditional Rendering of Views ---
    return (
        <div className="min-h-screen bg-gray-100 flex flex-col items-center justify-center py-10 px-4 font-sans">
            {/* Message Bar (always visible at the top) */}
            {message && (
                <div className={`fixed top-0 left-0 right-0 p-3 text-center text-sm font-medium z-50
                                ${message.startsWith('Error') ? 'bg-red-500 text-white' : 'bg-blue-500 text-white'}`}>
                    {message}
                </div>
            )}

            {!isDataIngested ? (
                // --- Ingestion Form View ---
                <div className="bg-white p-8 rounded-xl shadow-lg w-full max-w-2xl border border-gray-200">
                    <h1 className="text-3xl font-bold text-gray-800 mb-6 text-center">Converse with Your Data</h1>
                    <p className="text-gray-600 text-center mb-6">Start by uploading your CSV file and providing some basic information.</p>

                    <div className="mb-6">
                        <label htmlFor="csv-upload" className="block text-lg font-semibold text-gray-700 mb-2">
                            1. Upload CSV File
                        </label>
                        <input
                            type="file"
                            id="csv-upload"
                            accept=".csv"
                            onChange={handleFileChange}
                            className="block w-full text-sm text-gray-500
                                       file:mr-4 file:py-2 file:px-4
                                       file:rounded-full file:border-0
                                       file:text-sm file:font-semibold
                                       file:bg-indigo-50 file:text-indigo-700
                                       hover:file:bg-indigo-100 cursor-pointer"
                        />
                        {selectedFile && (
                            <p className="mt-2 text-sm text-gray-600">Selected: <span className="font-medium">{selectedFile.name}</span></p>
                        )}
                    </div>

                    <div className="mb-6">
                        <label htmlFor="table-name" className="block text-lg font-semibold text-gray-700 mb-2">
                            2. Table Name (for database queries)
                        </label>
                        <input
                            type="text"
                            id="table-name"
                            value={tableName}
                            onChange={(e) => setTableName(e.target.value)}
                            placeholder="e.g., my_sales_data"
                            className="block w-full px-4 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 text-base"
                        />
                    </div>

                    <div className="mb-6">
                        <label htmlFor="data-start-line" className="block text-lg font-semibold text-gray-700 mb-2">
                            3. Data Start Line (e.g., 1 if header is first line, 4 if there are 3 intro lines)
                        </label>
                        <input
                            type="number"
                            id="data-start-line"
                            value={dataStartLine}
                            onChange={(e) => setDataStartLine(e.target.value)}
                            placeholder="e.g., 1 or 4"
                            min="1"
                            className="block w-full px-4 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 text-base"
                        />
                    </div>

                    <button
                        onClick={handleIngestAndSuggest}
                        disabled={loading || !selectedFile || !tableName.trim() || !dataStartLine.trim()}
                        className="w-full bg-indigo-600 text-white py-3 px-6 rounded-lg text-lg font-semibold
                                   hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2
                                   transition duration-150 ease-in-out shadow-md disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        {loading ? 'Processing Data...' : 'Upload CSV & Start Analyzing'}
                    </button>
                </div>
            ) : (
                // --- Main Dashboard View (Ever-present Chatbot) ---
                <div className="flex flex-col md:flex-row min-h-[calc(100vh-60px)] w-full max-w-7xl gap-4">
                    {/* Left Panel: Chatbot & Visualization Selection */}
                    <div className="md:w-1/3 w-full bg-white p-6 rounded-xl shadow-lg border border-gray-200 flex flex-col space-y-6">
                        <h2 className="text-2xl font-bold text-gray-800 mb-4 text-center">AI Chatbot & Query Results</h2>
                        
                        {/* Chatbot Interface - directly embedded */}
                        <div className="flex-grow flex flex-col border border-gray-300 rounded-lg p-3 bg-gray-50 overflow-hidden">
                            <div className="flex-grow overflow-y-auto mb-3 p-2 border-b border-gray-200">
                                {chatHistory.length === 0 ? (
                                    <p className="text-gray-500 text-sm italic text-center">Ask a question about your data (e.g., "Show me sales by region").</p>
                                ) : (
                                    chatHistory.map((chatItem, index) => (
                                        <div key={index} className={`p-3 rounded-lg shadow-sm mb-2 ${
                                            chatItem.type === 'user' ? 'bg-indigo-100 text-indigo-800 self-end ml-auto' :
                                            chatItem.type === 'sql' ? 'bg-blue-100 text-blue-800' :
                                            chatItem.type === 'result' ? 'bg-green-100 text-green-800' :
                                            'bg-red-100 text-red-800' // For error messages
                                        }`}>
                                            {chatItem.type === 'user' && <p className="font-semibold">You:</p>}
                                            {chatItem.type === 'sql' && <p className="font-semibold">Generated SQL:</p>}
                                            {chatItem.type === 'error' && <p className="font-semibold">Error:</p>}

                                            {typeof chatItem.content === 'string' ? (
                                                <pre className="whitespace-pre-wrap font-mono text-sm">{chatItem.content}</pre>
                                            ) : (
                                                chatItem.type === 'result' && Array.isArray(chatItem.content) && chatItem.content.length > 0 ? (
                                                    <>
                                                        <div className="overflow-x-auto max-h-48">
                                                            <table className="min-w-full divide-y divide-gray-200 text-left text-xs">
                                                                <thead className="bg-gray-100">
                                                                    <tr>
                                                                        {Object.keys(chatItem.content[0]).map((key) => (
                                                                            <th key={key} className="px-3 py-2 font-medium text-gray-600 uppercase tracking-wider">
                                                                                {key}
                                                                            </th>
                                                                        ))}
                                                                    </tr>
                                                                </thead>
                                                                <tbody className="bg-white divide-y divide-gray-200">
                                                                    {chatItem.content.map((row, rowIndex) => (
                                                                        <tr key={rowIndex}>
                                                                            {Object.values(row).map((value, colIndex) => (
                                                                                <td key={colIndex} className="px-3 py-2 whitespace-nowrap text-gray-900">
                                                                                    {String(value)}
                                                                                </td>
                                                                            ))}
                                                                        </tr>
                                                                    ))}
                                                                </tbody>
                                                            </table>
                                                        </div>
                                                        {/* New Download CSV Button */}
                                                        <button
                                                            onClick={() => handleDownloadCsv(chatItem.content, `query_results_${Date.now()}.csv`)}
                                                            className="mt-2 w-full bg-purple-600 text-white py-1 px-3 rounded-md text-sm font-semibold
                                                                       hover:bg-purple-700 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-2
                                                                       transition duration-150 ease-in-out shadow-sm"
                                                        >
                                                            Download CSV
                                                        </button>
                                                    </>
                                                ) : (
                                                    <p className="text-gray-500 italic">No results returned or an issue occurred.</p>
                                                )
                                            )}
                                        </div>
                                    ))
                                )}
                                <div ref={chatHistoryEndRef} /> {/* For auto-scrolling */}
                            </div>
                            <textarea
                                id="user-query-input"
                                value={userQuery}
                                onChange={(e) => setUserQuery(e.target.value)}
                                placeholder="e.g., Show me the total sales for each region"
                                rows="3"
                                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm resize-none mb-2"
                            ></textarea>
                            <button
                                onClick={handleQueryData}
                                disabled={loading || !userQuery.trim()}
                                className="w-full bg-blue-600 text-white py-2 px-4 rounded-lg text-md font-semibold
                                           hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2
                                           transition duration-150 ease-in-out shadow-md disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                                {loading && message.startsWith('Processing natural') ? 'Querying...' : 'Run SQL Query'}
                            </button>
                        </div>

                        {/* Visualization Selection */}
                        {initialSuggestions.length > 0 && (
                            <div className="mt-6 border-t pt-6">
                                <h3 className="text-lg font-semibold text-gray-700 mb-3">Select Visualizations:</h3>
                                <div className="grid grid-cols-2 gap-3">
                                    {initialSuggestions.map((suggestion, index) => (
                                        <div key={index} className="flex items-center">
                                            <input
                                                type="checkbox"
                                                id={`viz-${suggestion.type}`}
                                                checked={selectedVizTypes.includes(suggestion.type)}
                                                onChange={() => handleVizTypeChange(suggestion.type)}
                                                className="h-4 w-4 text-green-600 focus:ring-green-500 border-gray-300 rounded"
                                            />
                                            <label htmlFor={`viz-${suggestion.type}`} className="ml-2 text-gray-700 text-sm font-medium cursor-pointer">
                                                {toTitleCase(suggestion.type.replace('_', ' ').replace('CHART', 'Chart').replace('PLOT', 'Plot'))}
                                                <p className="text-xs text-gray-500">{suggestion.reason}</p>
                                            </label>
                                        </div>
                                    ))}
                                </div>
                                <button
                                    onClick={handleGenerateVisualizations}
                                    disabled={loading || selectedVizTypes.length === 0}
                                    className="mt-4 w-full bg-green-600 text-white py-2 px-4 rounded-lg text-md font-semibold
                                               hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2
                                               transition duration-150 ease-in-out shadow-md disabled:opacity-50 disabled:cursor-not-allowed"
                                >
                                    {loading && message.startsWith('Generating') ? 'Generating Plots...' : 'Generate Selected Visualizations'}
                                </button>
                            </div>
                        )}
                    </div>

                    {/* Right Main Content Area: Raw Data Table (Top) + Plots (Bottom) */}
                    <div className="md:w-2/3 w-full flex flex-col gap-4">
                        {/* Raw Data Table (Top Right) */}
                        <div className="bg-white p-4 rounded-xl shadow-lg border border-gray-200 flex-none">
                            <h2 className="text-2xl font-bold text-gray-800 mb-4 text-center">Raw Data Table</h2>
                            <div className="h-48 overflow-auto">
                                <RawDataTable data={fullRawData} />
                            </div>
                        </div>

                        {/* Plots Area (Bottom Right) */}
                        <div className="bg-white p-6 rounded-xl shadow-lg border border-gray-200 flex-grow flex flex-col overflow-auto">
                            <h2 className="text-2xl font-bold text-gray-800 mb-4 text-center">Visualizations</h2>

                            {generatedPlots.length > 0 ? (
                                <div className="grid grid-cols-1 gap-6">
                                    {generatedPlots.map((plotData, index) => (
                                        <div key={index} className="bg-gray-50 p-4 rounded-lg shadow-inner border border-gray-200">
                                            <Plot
                                                data={plotData.data}
                                                layout={plotData.layout}
                                                config={{ responsive: true }}
                                                style={{ width: '100%', height: '400px' }}
                                            />
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="flex-grow flex items-center justify-center text-gray-500 text-lg italic">
                                    Your visualizations will appear here after selection.
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default App;
