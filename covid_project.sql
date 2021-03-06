
SELECT Location, date, total_cases, new_cases, total_deaths, population
 FROM CovidDeaths
 ORDER BY 1,2;

-- looking at Total Cases vs Total Deaths 
-- shows likelihood of dying if you contract covid in your country 
SELECT Location, date, total_cases, new_cases, total_deaths, 
 (total_deaths/total_cases)*100 as DeathPercentage
 FROM CovidDeaths
 WHERE location like '%States%'
 ORDER BY 1,2;

-- looking at Total cases vs population
-- shows what percentage of population got covid 
SELECT Location, date, population, total_cases, (total_cases/population)*100 as PercentPopulationInfected
 FROM CovidDeaths
 WHERE location like '%States%'
 ORDER BY 1,2;

-- looking at countries with highest infection rate compared to the population
SELECT Location, population, MAX(total_cases) as HighestInfectionCount, MAX((total_cases/population))*100 as PercentPopulationInfected
 FROM CovidDeaths
 --WHERE location like '%States%'
 GROUP BY Location, population
 ORDER BY PercentPopulationInfected DESC;


 -- showing countries with highest death count per population
SELECT Location, MAX(cast(total_deaths as int)) as TotalDeathCount
 FROM CovidDeaths
 --WHERE location like '%States%'
 WHERE continent is not NULL AND total_deaths is not NULL
 GROUP BY Location
 ORDER BY TotalDeathCount DESC;


SELECT location, MAX(cast(total_deaths as int)) as TotalDeathCount
 FROM CovidDeaths
 --WHERE location like '%States%'
 WHERE continent is NULL AND location NOT LIKE '%income%'
 GROUP BY location
 ORDER BY TotalDeathCount DESC;


 -- LET'S BREAK IT DOWN BY CONTINENT 
 
SELECT continent, MAX(cast(total_deaths as int)) as TotalDeathCount
 FROM CovidDeaths
 --WHERE location like '%States%'
 WHERE continent is not NULL AND total_deaths is not NULL
 GROUP BY continent
 ORDER BY TotalDeathCount DESC;


-- showing continents with the highest death count per population

SELECT continent, MAX(cast(total_deaths as int)) as TotalDeathCount
 FROM CovidDeaths
 --WHERE location like '%States%'
 WHERE continent is not NULL AND total_deaths is not NULL
 GROUP BY continent
 ORDER BY TotalDeathCount DESC;

-- GLOBAL NUMBERS

SELECT SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(new_cases) *100 as DeathPercentage
 FROM CovidDeaths
 --WHERE location like '%States%'
 WHERE continent is not NULL
 --GROUP BY date
 ORDER BY 1,2;



 ------------------------------------------ looking at table: vaccinations

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
 SUM(CAST(vac.new_vaccinations as int)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated 
 FROM coviddeaths dea
 JOIN covidvaccinations vac
    ON dea.location = vac.location
    AND dea.date = vac.date
 WHERE dea.continent is not NULL
 ORDER BY 2,3;

-- USE CTE
WITH PopvsVac (Continent, Location, Date, Population, new_vaccinations, RollingPeopleVaccinated)
 as (
     SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
        SUM(CAST(vac.new_vaccinations as int)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated 
        FROM coviddeaths dea
        JOIN covidvaccinations vac
            ON dea.location = vac.location
            AND dea.date = vac.date
        WHERE dea.continent is not NULL
        --ORDER BY 2,3
    )
    SELECT *, (RollingPeopleVaccinated/population)*100
    FROM PopvsVac;

CREATE VIEW percpopulationvaccinated as 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
SUM(CAST(vac.new_vaccinations as int)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated 
FROM coviddeaths dea
JOIN covidvaccinations vac
    ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent is not NULL;
--ORDER BY 2,3;

