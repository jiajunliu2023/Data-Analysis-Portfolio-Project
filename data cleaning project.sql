-- Data Cleaning

use world_layoffs;

select * 
from layoffs;

-- 1. remove any duplicates
-- 2. Standardize the Data (if there are some misspelling etc)
-- 3. NULL values or blank values
-- 4. Remove any columns (irrelevant)

-- copy all variables from layoffs table to layoffs_staging table (intially the layoffs_staging table is empty)
create table layoffs_staging 
like layoffs;

select * 
from layoffs_staging;

-- insert all data from layoffs table to layoffs_staging table 
insert layoffs_staging
select *
from layoffs;

select * 
from layoffs;

-- create a variable name called row_num to count the occurrance of each row of data
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;


-- if the row numbers is greater than 1, that means there is a duplication
WITH duplicated_cte as 
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicated_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Atlassian';

-- 1, remove the duplications
-- The first way to delete the duplication
delete 
from duplicated_cte
where row_num > 1;


-- The second way to delete the duplication
CREATE TABLE `layoffs_staging1` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- insert all data in layoffs_staging to layoffs_staging1 table
insert into layoffs_staging1
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- delete the duplication (before delete duplications, check the duplications with 'select *')

select *
from layoffs_staging1
where row_num > 1;

delete
from layoffs_staging1
where row_num > 1;

-- then, layoffs_staging1 table will be used for further data cleaning after removing the duplication


-- 2. standardizing data

-- check different company 
select distinct(company)
from layoffs_staging1;

-- remove the spaces of company name
-- 1. check the unstandarizing data 
select company, trim(company)
from layoffs_staging1;

-- 2. remove the unnecessary spaces
update layoffs_staging1
set company = trim(company);

-- show different industry
select distinct(industry)
from layoffs_staging1
order by 1;

-- update the company in the crypto sth industry to just crypto industry
update layoffs_staging1
set industry = 'Crypto'
where industry like 'Crypto%';



-- remove the "." at the end 
select distinct country, trim(trailing "." from country)
from layoffs_staging1
order by 1;

update layoffs_staging1
set country = trim(trailing "." from country)
where country like 'united states%';

-- Convert the date variable from text to date format of data (still the text)
select `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
from layoffs_staging1;

update layoffs_staging1
set `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

select `date`
from layoffs_staging1;


-- Change the data type of the actual data (do it on staging table, not the original table)
-- change the date variable from a text type to a date type of data
alter table layoffs_staging1
modify column `date` date;

-- 3.  NULL values or blank values
select * 
from layoffs_staging1
where total_laid_off is null and percentage_laid_off is null;

select *
from layoffs_staging1
where company = 'Airbnb';

-- if there are two data about the same company with one with the industry name and one without the industry name. 
-- we can populate it by updating the industry name for the one without the industry name

select t1.industry,t2.industry
from layoffs_staging1 t1
join layoffs_staging1 t2
	on t1.company = t2.company
	and t1.location = t2.location
where (t1.industry is null or t1.industry = '') and t2.industry is not null;

-- updating the industry name for the one without the industry name
update layoffs_staging1 t1
join layoffs_staging1 t2
	on t1.company = t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;

-- 4. Remove any columns (irrelevant)
-- the rows that do not have values for both total_laid_off and percentage_laid_off
select * 
from layoffs_staging1
where total_laid_off is null and percentage_laid_off is null;

-- delete rows that do not have values for both total_laid_off and percentage_laid_off 
delete
from layoffs_staging1
where total_laid_off is null and percentage_laid_off is null;

-- after using row_num column for identifing the duplication, we do not need the row_num and we drop it.
alter table layoffs_staging1
drop column row_num;












