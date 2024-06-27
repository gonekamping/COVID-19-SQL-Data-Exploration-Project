--- DATABASE SETUP ---
-- Create "CovidDeaths" table.  Ensuring the date type for each column is useful for analysis.
CREATE TABLE CovidDeaths
(iso_code VARCHAR, 
 continent VARCHAR, 
 location VARCHAR, 
 date DATE, 
 total_cases NUMERIC, 
 new_cases NUMERIC, 
 new_cases_smoothed NUMERIC,
 total_deaths NUMERIC,
 new_deaths NUMERIC,
 new_deaths_smoothed NUMERIC,
 total_cases_per_million NUMERIC,
 new_cases_per_million NUMERIC,
 new_cases_smoothed_per_million NUMERIC,
 total_deaths_per_million NUMERIC,
 new_deaths_per_million NUMERIC,
 new_deaths_smoothed_per_million NUMERIC,
 reproduction_rate NUMERIC,
 icu_patients NUMERIC,
 icu_patients_per_million NUMERIC,
 hosp_patients NUMERIC,
 hosp_patients_per_million NUMERIC,
 weekly_icu_admissions NUMERIC,
 weekly_icu_admissions_per_million NUMERIC,
 weekly_hosp_admissions NUMERIC,
 weekly_hosp_admissions_per_million NUMERIC,
 new_tests NUMERIC,
 total_tests NUMERIC,
 total_tests_per_thousand NUMERIC,
 new_tests_per_thousand NUMERIC,
 new_tests_smoothed NUMERIC,
 new_tests_smoothed_per_thousand NUMERIC,
 positive_rate NUMERIC,
 tests_per_case NUMERIC,
 tests_units VARCHAR,
 total_vaccinations NUMERIC,
 people_vaccinated NUMERIC,
 people_fully_vaccinated NUMERIC,
 new_vaccinations NUMERIC,
 new_vaccinations_smoothed NUMERIC,
 total_vaccinations_per_hundred NUMERIC,
 people_vaccinated_per_hundred NUMERIC,
 people_fully_vaccinated_per_hundred NUMERIC,
 new_vaccinations_smoothed_per_million NUMERIC,
 stringency_index NUMERIC,
 population NUMERIC,
 population_density NUMERIC,
 median_age NUMERIC,
 aged_65_older NUMERIC,
 aged_70_older NUMERIC,
 gdp_per_capita NUMERIC,
 extreme_poverty NUMERIC,
 cardiovasc_death_rate NUMERIC,
 diabetes_prevalence NUMERIC,
 female_smokers NUMERIC,
 male_smokers NUMERIC,
 handwashing_facilities NUMERIC,
 hospital_beds_per_thousand NUMERIC,
 life_expectancy NUMERIC,
 human_development_index NUMERIC
);

-- Create "CovidVaccinations" table.  Ensuring the date type for each column is useful for analysis.
CREATE TABLE CovidVaccinations
(iso_code VARCHAR,
 continent VARCHAR,
 location VARCHAR,
 date DATE,
 new_tests NUMERIC,
 total_tests NUMERIC,
 total_tests_per_thousand NUMERIC,
 new_tests_per_thousand NUMERIC,
 new_tests_smoothed NUMERIC,
 new_tests_smoothed_per_thousand NUMERIC,
 positive_rate NUMERIC,
 tests_per_case NUMERIC,
 tests_units VARCHAR,
 total_vaccinations NUMERIC,
 people_vaccinated NUMERIC,
 people_fully_vaccinated NUMERIC,
 new_vaccinations NUMERIC,
 new_vaccinations_smoothed NUMERIC,
 total_vaccinations_per_hundred NUMERIC,
 people_vaccinated_per_hundred NUMERIC,
 people_fully_vaccinated_per_hundred NUMERIC,
 new_vaccinations_smoothed_per_million NUMERIC,
 stringency_index NUMERIC,
 population_density NUMERIC,
 median_age NUMERIC,
 aged_65_older NUMERIC,
 aged_70_older NUMERIC,
 gdp_per_capita NUMERIC,
 extreme_poverty NUMERIC,
 cardiovasc_death_rate NUMERIC,
 diabetes_prevalence NUMERIC,
 female_smokers NUMERIC,
 male_smokers NUMERIC,
 handwashing_facilities NUMERIC,
 hospital_beds_per_thousand NUMERIC,
 life_expectancy NUMERIC,
 human_development_index NUMERIC
);

-- Load data to the tables using the Import/Export function from the sourced .csv files.

-- Review data in each table to ensure all is loaded as expected.
SELECT * FROM CovidDeaths;
SELECT * FROM CovidVaccinations;

-- Select the data that will be used.
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM CovidDeaths
WHERE continent IS NOT NULL -- To filter out any continent groupings.
ORDER BY location, date;


--- TOTALS ---
-- Total Cases vs. Total Deaths - Answering "What is the likelihood of death for each country each day?".
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_percentage
FROM CovidDeaths
WHERE continent IS NOT NULL -- To filter out any continent groupings.
ORDER BY location, date;

-- Total Cases vs. Population - Answering "What is the percentage of the population of each country that has COVID19 each day?".
SELECT location, date, population, total_cases, (total_cases/population)*100 AS percent_population_infected
FROM CovidDeaths
WHERE continent IS NOT NULL -- To filter out any continent groupings.
ORDER BY location, date;


--- COUNTRY NUMBERS ---
-- Countries with the highest infection rate compared to its population.
SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 as percent_population_infected
FROM CovidDeaths
WHERE continent IS NOT NULL -- To filter out any continent groupings.
AND population IS NOT NULL -- To remove possible null calculations.
AND total_cases IS NOT NULL -- To remove possible null calculations.
GROUP BY location, population
ORDER BY percent_population_infected DESC;

-- Countries with highest death counts.
SELECT location, MAX(total_deaths) as total_death_count
FROM CovidDeaths
WHERE continent IS NOT NULL -- To filter out any continent groupings.
AND total_deaths IS NOT NULL  -- To remove possible null calculations.
GROUP BY Location
ORDER BY total_death_count DESC;


--- CONTINENT NUMBERS ---
-- ***Continents with highest death counts.
SELECT location, MAX(total_deaths) AS total_death_count
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY total_death_count DESC;

-- Continents with highest death counts.
SELECT continent, MAX(total_deaths) AS total_death_count
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY total_death_count DESC;


--- GLOBAL NUMBERS ---
-- Global Total Cases vs. Total Deaths - Asnwering "What is the likelihood of death across the globe on each date?".
SELECT date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 as death_percentage
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY date;

-- Global Total Cases vs. Total Deaths - Answering "What is the likelihood of death across the globe?".
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 as death_percentage
FROM CovidDeaths
WHERE continent IS NOT NULL;

-- Total Population vs. Vaccinations - Answering "What percentage of a country's population is vaccinated each day?" and "What percentage of total population vaccinated up to that day?".
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaccinated
FROM CovidDeaths AS dea
JOIN CovidVaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY location, date;

	-- CTE Option - Temporarily stores the query returns as a Common Expression Table.
	WITH PopVsVac (continent, location, date, population, new_vaccinations, rolling_people_vaccinated)
	AS
	(
	SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaccinated
	FROM CovidDeaths AS dea
	JOIN CovidVaccinations AS vac
		ON dea.location = vac.location
		AND dea.date = vac.date
	WHERE dea.continent IS NOT NULL
	ORDER BY location, date
	)
	SELECT *, (rolling_people_vaccinated/population)*100 AS percentage_vaccinated_population
	FROM PopVsVac
  
  -- Temp Table - Temoporarily stores the query as a new table.
	DROP TABLE IF EXISTS PecentPopulationVaccinated
	CREATE TABLE PercentPopulationVaccinated
	(
	continent varchar,
	location varchar,
	date date,
	population numeric,
	new_vaccinations numeric,
	rolling_people_vaccinated numeric
	)
	INSERT INTO PercentPopulationVaccinated
	(SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaccinated
	FROM CovidDeaths AS dea
	JOIN CovidVaccinations AS vac
		ON dea.location = vac.location
		AND dea.date = vac.date
	WHERE dea.continent IS NOT NULL
	ORDER BY location, date
	)
	SELECT *, (rolling_people_vaccinated/population)*100 AS percentage_vaccinated_population
	FROM PercentPopulationVaccinated
	
--- VIEWS ---
-- Create a view of the Percent Population Vaccinated table created for future reference.
CREATE VIEW PercentPopulationVaccinatedView AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaccinated
FROM CovidDeaths AS dea
JOIN CovidVaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL


