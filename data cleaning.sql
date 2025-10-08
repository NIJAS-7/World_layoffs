-- DATA CLEANING

SELECT *
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null value or blank values
-- 4. Remove any columns


CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT *
FROM layoffs_staging;


INSERT INTO layoffs_staging
SELECT *
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,total_laid_off,percentage_laid_off,industry,stage,funds_raised,country,`date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;



SELECT *
FROM layoffs_staging
WHERE company='ola';



WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,total_laid_off,percentage_laid_off,industry,stage,funds_raised,country,`date`) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;




SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,total_laid_off,percentage_laid_off,industry,stage,funds_raised,country,`date`) AS row_num
FROM layoffs_staging;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,total_laid_off,percentage_laid_off,industry,stage,funds_raised,country,`date`) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num >1;


SELECT *
FROM layoffs_staging2;

-- standardizing data
SELECT DISTINCT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);


SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE "crypto%";


SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2;

-- NULL VALUES OR BLANK VALUES

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT *
FROM  layoffs_staging2
WHERE company LIKE 'appsmith';

SELECT *
FROM  layoffs_staging2 t1
JOIN  layoffs_staging2 t2
    ON t1.company =  t2.company
WHERE (t1.industry IS NULL OR t1.industry='')   
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
OR total_laid_off = '';

 UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = '';

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL
OR percentage_laid_off  = '';

UPDATE layoffs_staging2
SET funds_raised = NULL
WHERE  funds_raised = '';

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL
AND total_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL
AND total_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;























































