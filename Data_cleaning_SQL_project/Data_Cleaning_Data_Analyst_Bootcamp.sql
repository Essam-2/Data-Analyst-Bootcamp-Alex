-- Data Cleaning 

SELECT * 
FROM layoffs;

-- 1. Remove Deplicates if any 
-- 2. Standardize the Data 
-- 3. Null Values or blank values 
-- 4. Remove any columns 


CREATE TABLE layoffs_staging 
Like layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging SELECT * from layoffs;

SELECT * 
FROM layoffs_staging;


-- 1. Remove Deplicates if any 
	-- There is no ID column to use 

-- Add new row_num column by using ROW_NUMBER() OVER()
-- ROW_NUMBER(): Generates a unique row number starting from 1 for each row.
-- Inside OVER()
	-- PARTITION BY column1, column2: (Optional) Divides rows into groups before assigning row numbers.
	-- ORDER BY column3: Specifies the order in which row numbers are assigned within each partition.

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;


WITH deplicate_cte AS 
(
SELECT *, 
	ROW_NUMBER() OVER(
	PARTITION BY 
    company, 
    location,
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    `date`, 
    stage, 
    country, 
    funds_raised_millions) AS row_num
	FROM layoffs_staging
)
SELECT * 
FROM deplicate_cte 
WHERE row_num > 1; 

-- here we check if this entity is really duplicate 
SELECT * 
FROM layoffs_staging 
WHERE company = 'Casper';



WITH deplicate_cte AS 
(
SELECT *, 
	ROW_NUMBER() OVER(
	PARTITION BY 
    company, 
    location,
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    `date`, 
    stage, 
    country, 
    funds_raised_millions) AS row_num
	FROM layoffs_staging
)
DELETE 
FROM deplicate_cte 
WHERE row_num > 1; 


CREATE TABLE `layoffs_staging2` (
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


SELECT * 
FROM layoffs_staging2;



INSERT into layoffs_staging2
SELECT *, 
	ROW_NUMBER() OVER(
	PARTITION BY 
    company, 
    location,
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    `date`, 
    stage, 
    country, 
    funds_raised_millions) AS row_num
	FROM layoffs_staging;



SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2;


-- Standardizing data (Remove white spaces)

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

-- Standardizing data (Change 'CryptoCurrency' to 'Crypto' )
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';



SELECT distinct(industry)
FROM layoffs_staging2
order by 1;


-- Standardizing data (Remove '.')

SELECT distinct country, trim(TRAILING '.' from country) as country_
FROM layoffs_staging2
ORDER BY -1;

UPDATE layoffs_staging2
SET country = trim(TRAILING '.' from country)
WHERE country LIKE 'United States%';


SELECT * 
from layoffs_staging2
order by 1;


-- Standardizing data (Convarte 'Date' from text to Date Fromate )

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2;


UPDATE layoffs_staging2 
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

Select * from layoffs_staging2 ;


-- Standardizing data (Convarte 'Date' from text to Date Type )

ALTER TABLE layoffs_staging2 
modify column `date` DATE;


-- Fix Null and Blank values 
-- to find Null values uses 'IS' not '='


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';


-- Populate data where it could be 


SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


SELECT t1.industry, t2.industry
from layoffs_staging2 as t1 
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
    AND t1.location = t2.location 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;

UPDATE layoffs_staging2 as t1
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;

-- Will set Null in the blank value for industry 
-- Now all the blanck in the industry column will be Null 
UPDATE layoffs_staging2
SET industry = NULL 
where industry = '';


-- Delete total_laid_off and percentage_laid_off when both are null

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Delete row_num column 
ALTER TABLE layoffs_staging2 
DROP COLUMN row_num;



SELECT * 
from layoffs_staging2;