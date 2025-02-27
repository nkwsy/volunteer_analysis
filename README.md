# Park Volunteer Analysis Tool

A Streamlit application for analyzing volunteer locations and distances for park management.

## Features

- Interactive map visualization of volunteer locations
- Distance analysis from reference points
- Heatmap visualization
- Volunteer data management and export

## Setup

### Prerequisites

- Python 3.8 or higher
- Git

### Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd volunteer_analysis
   ```

2. Create a virtual environment:
   ```
   # On Windows
   python -m venv env
   env\Scripts\activate

   # On macOS/Linux
   python -m venv env
   source env/bin/activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

### Running the Application

1. Activate the virtual environment (if not already activated):
   ```
   # On Windows
   env\Scripts\activate

   # On macOS/Linux
   source env/bin/activate
   ```

2. Run the Streamlit app:
   ```
   streamlit run volunteer_analysis_app.py
   ```

3. Open your browser and navigate to the URL shown in the terminal (typically http://localhost:8501)

## Data Format

The application expects GeoJSON data with volunteer information in the following format:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "name": "Volunteer Name",
        "email": "volunteer@example.com",
        "address": "123 Main St, City, State"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [-87.6244, 41.9067]
      }
    }
  ]
}
```

A sample file named `addresses.geojson` can be placed in the project root directory, or you can upload your own file through the application interface.

## License

[MIT License](LICENSE) 