# IMDb Dataset Analysis Project

A comprehensive analysis of IMDb datasets using Apache Spark for Big Data processing and analysis.

This university project was developed as part of the "Big Data Analysis" course.

## Project Overview

This project aims to analyze the IMDb datasets to derive insights about movies, TV shows, actors, directors, and other entertainment industry data. The analysis is performed using Apache Spark for big data processing, with comprehensive data cleaning, transformation, and analysis operations.


## Team Members

This project was developed as part of the "Big Data Analysis" course by:

- Anton Fadeev
- Arsen Badia
- Anton Lavreniuk
- Andrian Voznyi
- Yuliia Lekhiv

## Team Collaboration

The project was carried out collaboratively by all team members, each contributing to different aspects of the implementation. Anton Fadeev was responsible for configuring and installing PySpark as well as setting up the initial project structure. Arsen Badia implemented Docker support and handled technical environment configuration. Anton Lavreniuk took charge of the overall project structure, maintained the Git repository, and was also responsible for saving the results and debugging common issues. Yuliia Lekhiv focused on data cleaning, fixing inconsistencies, and co-created the final presentation with Andrian Voznyi. Andrian also resolved technical errors and led the presentation design. Each team member worked independently on their own set of analytical questions and contributed general improvements to the project infrastructure and results.


## Data Sources

The project uses the official IMDb dataset files in TSV.GZ format:

- `name.basics.tsv.gz` - Information about people (actors, directors, etc.)
- `title.akas.tsv.gz` - Alternative titles for movies and shows
- `title.basics.tsv.gz` - Basic information about titles (movies, shows, etc.)
- `title.crew.tsv.gz` - Directors and writers for titles
- `title.episode.tsv.gz` - Episode information for TV series
- `title.principals.tsv.gz` - Principal cast/crew for titles
- `title.ratings.tsv.gz` - IMDb ratings for titles

## Project Structure

```
├── analyzer.py              # Core analysis functions
├── Dockerfile               # Docker configuration
├── main.py                  # Main application entry point
├── reader.py                # Data loading functions
├── requirements.txt         # Python dependencies
├── schemas.py               # Dataset schema definitions
├── utils.py                 # Utility functions
├── imdb-data/               # Raw dataset files
└── questions/               # Analysis questions by team members
    ├── andrian_v_questions.py
    ├── anton_f_questions.py
    ├── anton_l_questions.py
    ├── arsen_b_questions.py
    └── yuliia_l_questions.py
```

## Features

The project implements various data analysis techniques:

1. **Data Loading**: Loading and parsing multiple IMDb datasets with proper schema definitions
2. **Data Cleaning**: Handling null values, filtering invalid entries, and preparing data for analysis
3. **Exploratory Data Analysis**: Statistical analysis and visualization of datasets
4. **Complex Queries**: Implementation of various analytical questions using:
   - Filtering operations
   - Joining multiple datasets
   - Aggregation and grouping
   - Window functions for ranking and statistics

## Student Contributions

Each team member focused on different analytical questions:

### Anton Fadeev
- Analysis of movie runtimes and ratings
- Filtering by movie length, release year, and genre
- Analysis of top-rated movies per year
- Ranking movies by votes and popularity

### Arsen Badia
- Analysis of Ukrainian-language titles
- Documentary film analysis
- Analysis of movie ratings by decade
- Finding top movies per genre

### Andrian Voznyi
- Analysis of recent movies (2020+)
- Animated movie analysis
- Language distribution in titles
- Actor ranking by movie count

### Anton Lavreniuk
- Director and writer analysis
- Identifying directors with highest-rated movies
- Analysis of directors who are also writers
- Comparative analysis of film ratings by director

### Yuliia Lekhiv
- Analysis of movie runtimes by genre
- Regional analysis of TV episodes
- Director-specific analysis (e.g., Steven Spielberg)
- Ranking actors by episode count

## Technical Implementation

- **Apache Spark**: Used for distributed data processing
- **PySpark**: Python API for Spark
- **Data Visualization**: Matplotlib and Seaborn for visualizations
- **Docker**: Containerization for reproducible environments

## How to Run

1. Ensure you have the IMDb dataset files in the `imdb-data/` directory
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Run the analysis:
   ```
   python main.py
   ```

## Outputs

Analysis results are saved in the `outputs/` directory, organized by team member:
- `/outputs/anton_f/` - Anton Fadeev's analysis results
- `/outputs/arsen_b/` - Arsen Badia's analysis results
- `/outputs/andrian_v/` - Andrian Voznyi's analysis results
- `/outputs/anton_l/` - Anton Lavreniuk's analysis results
- `/outputs/yuliia_l/` - Yuliia Lekhiv's analysis results

Results include CSV files with query results and visualization images.

## Docker Support

The project includes Docker support for running in a containerized environment:

```
docker build -t my-spark-img .
docker run my-spark-img
```