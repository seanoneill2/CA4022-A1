-- File containing pig code to complete queries

-- 1. First pig query to find most calories from drinks file

drinks = LOAD 'starbucks-clean-data/clean-drinks/part-m-00000' USING PigStorage(',') AS (name:chararray, calories:int, fat:int, carb:int, fiber:int, protein:int, sodium:int);

sorted_calories = ORDER drinks BY calories DESC, name;
top_five_drinks = LIMIT sorted_calories 5;
top_five = FOREACH top_five_drinks GENERATE name, calories;
DUMP top_five;

-- 2. Next query to find what drink has most variations from extended drinks file

drinks_extended = LOAD 'starbucks-clean-data/clean-drinks-extended/part-m-00000' USING PigStorage(',') AS (
    beverage_category:chararray, beverage:chararray, beverage_prep:chararray,
    calories:int, total_fat:float, trans_fat:float, saturated_fat:float,
    sodium:int, total_carbohydrates:int, cholesterol:int,
    dietary_fibre:int, sugars:int, protein:float,
    vitamin_a:chararray, vitamin_c:chararray, calcium:chararray,
    iron:chararray, caffeine:int);

-- Group drinks by beverage first, before finding top 5 
grouped_drinks = GROUP drinks_extended BY beverage;
drink_prep_counts = FOREACH grouped_drinks GENERATE group AS beverage, COUNT(drinks_extended.beverage_prep) AS prep_count;
sorted_drinks = ORDER drink_prep_counts BY prep_count DESC, beverage;
top_beverages = LIMIT sorted_drinks 5;
DUMP top_beverages_name;

-- 3. Lastly, find drinks with fewest variations

grouped_drinks = GROUP drinks_extended BY beverage;
drink_prep_counts = FOREACH grouped_drinks GENERATE group AS beverage, COUNT(drinks_extended.beverage_prep) AS prep_count;
sorted_drinks = ORDER drink_prep_counts BY prep_count ASC, beverage;
bottom_beverages = LIMIT sorted_drinks 5;
DUMP bottom_beverages;

-- This completes my simple Pig queries