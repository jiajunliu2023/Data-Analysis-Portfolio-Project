-- Exploratory Data Analysis

USE world_layoffs;

SELECT max(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging1;

-- 100% percentage_laid_off
select * 
from layoffs_staging1
where percentage_laid_off = 1
order by total_laid_off desc;

select * 
from layoffs_staging1
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- show the total laid off for each company
SELECT company, sum(total_laid_off)
from layoffs_staging1
group by company 
order by 2 desc;

-- show the industry with the most laid off
SELECT industry, sum(total_laid_off)
from layoffs_staging1
group by industry
order by 2 desc;

-- the country with the most layoff
SELECT country, sum(total_laid_off)
from layoffs_staging1
group by country
order by 2 desc;

-- the number of layoff per day
SELECT `date`, sum(total_laid_off)
from layoffs_staging1
group by `date`
order by 2 desc;

-- the number of layoff per year
SELECT year(`date`), sum(total_laid_off)
from layoffs_staging1
group by year(`date`)
order by 2 desc;

-- the number of layoff from different stages of companies
SELECT stage, sum(total_laid_off)
from layoffs_staging1
group by stage
order by 2 desc;


-- the earliest and latest layoff
select min(`date`), max(`date`)
from layoffs_staging1; 


-- extract the month and year from the date and get the sum of layoff for each time period
select substring(`date`,1,7) as `Month` ,sum(total_laid_off)
from layoffs_staging1
where substring(`date`,1,7) is not null
group by `Month`
order by 1 asc;


-- sum up each month layoff
with rolling_total as
(
select substring(`date`,1,7) as `Month` ,sum(total_laid_off) as total_layoff
from layoffs_staging1
where substring(`date`,1,7) is not null
group by `Month`
order by 1 asc
)
select `Month`, total_layoff, sum(total_layoff) over(order by `Month`) rolling_total
from rolling_total;

-- show the number of layoff each year for the company
SELECT company, year(`date`),sum(total_laid_off)
from layoffs_staging1
group by company,year(`date`)  
order by company asc;

-- show the most layoff eeach year for the company
SELECT company, year(`date`),sum(total_laid_off)
from layoffs_staging1
group by company,year(`date`)  
order by 3 desc;

-- dense_ranking: 1, 2, 2, 3, 3, 4, 5
-- it shows for each year the top 5 companies to lay people off 
-- the first cte is to format the database table 
-- the second cte is to rank top companies each year to lay people off
with company_year (company, years, total_lay_off) as (
SELECT company, year(`date`),sum(total_laid_off)
from layoffs_staging1
group by company,year(`date`) 
) , Company_Year_Rank as (
select *, dense_rank() over (partition by years order by total_lay_off desc) as ranking
from company_year
where years is not null
)
select *
from Company_Year_Rank
where ranking <= 5;


