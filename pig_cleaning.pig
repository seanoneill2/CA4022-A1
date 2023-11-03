-- File containing Pig code to clean data

-- First I cleaned my drinks data

--Load it in

drinks = LOAD 'starbucks-raw-data/starbucks-menu-nutrition-drinks.csv' USING PigStorage(',') AS (name:chararray, calories:chararray, fat:chararray, carb:chararray, fiber:chararray, protein:chararray, sodium:chararray);

-- Remove header and '-' rows

filtered_drinks = FILTER drinks BY NOT (calories == '-' OR fat == '-' OR carb == '-' OR fiber == '-' OR protein == '-' OR sodium == '-');

filtered_drinks_end = FILTER filtered_drinks BY NOT (name =='' OR calories == 'Calories' OR fat == 'Fat (g)' OR carb == 'Carb. (g)' OR fiber == 'Fiber (g)' OR protein == 'Protein' OR sodium == 'Sodium');

-- Save to ints and floats

drinks_to_int = FOREACH filtered_drinks_end GENERATE name,  (int)calories AS calories, (float)fat AS fat,  (int)carb AS carb, (int)fiber AS fiber, (int)protein AS protein,  (int)sodium AS sodium;

-- Save file

STORE drinks_to_int INTO 'starbucks-clean-data/clean-drinks' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Food file cleaning

food = LOAD 'starbucks-raw-data/starbucks-menu-nutrition-food.csv' USING PigStorage(',') AS (name:chararray, calories:chararray, fat:chararray, carb:chararray, fiber:chararray, protein:chararray);

filtered_food = FILTER food BY NOT (TRIM(name) == '' OR TRIM(calories) == 'Calories' OR TRIM(fat) == 'Fat (g)' OR TRIM(carb) == 'Carb. (g)' OR TRIM(fiber) == 'Fiber (g)' OR TRIM(protein) == 'Protein');

foods_to_int = FOREACH filtered_food GENERATE name,  (int)calories AS calories, (float)fat AS fat,  (int)carb AS carb, (int)fiber AS fiber, (int)protein AS protein;


STORE foods_to_int INTO 'starbucks-clean-data/clean-food' USING org.apache.pig.piggybank.storage.CSVExcelStorage();


-- Extended drinks file cleaning

 drinks_extended = LOAD 'starbucks-raw-data/starbucks_drinkMenu_expanded.csv' USING PigStorage(',') AS (
    beverage_category:chararray, beverage:chararray, beverage_prep:chararray,
    calories:chararray, total_fat:chararray, trans_fat:chararray, saturated_fat:chararray,
    sodium:chararray, total_carbohydrates:chararray, cholesterol:chararray,
    dietary_fibre:chararray, sugars:chararray, protein:chararray,
    vitamin_a:chararray, vitamin_c:chararray, calcium:chararray,
    iron:chararray, caffeine:chararray
);

filtered_drinks_extended = FILTER drinks_extended BY NOT (beverage_category == 'Beverage_Category' OR beverage == 'Beverage' OR beverage_prep == 'Beverage_Prep' OR total_fat == 'Total Fat (g)' OR trans_fat == 'Trans Fat (g)' OR saturated_fat == 'Saturated Fat (g)' OR sodium == 'Sodium (mg)' OR total_carbohydrates == 'Total Carbohydrates (g)' OR cholesterol == 'Cholesterol (mg)' OR dietary_fibre == 'Dietary Fibre (g)' OR sugars == 'Sugars (g)' OR protein == 'Protein (g)' OR vitamin_a == 'Vitamin A (% DV)' OR vitamin_c == 'Vitamin C (% DV)' OR calcium == 'Calcium (% DV)' OR iron == 'Iron (% DV)' OR caffeine == 'Caffeine (mg)');

drinks_extended_converted = FOREACH filtered_drinks_extended GENERATE beverage_category, beverage, beverage_prep,  (int)calories AS calories, (float)total_fat AS total_fat, (float)trans_fat AS trans_fat, (float)saturated_fat AS saturated_fat, (int)sodium AS sodium,  (int)total_carbohydrates AS carb, (int)cholesterol AS cholesterol,  (int)dietary_fibre AS dietary_fibre,  (int)sugars,  (float)protein AS protein, vitamin_a, vitamin_c, calcium,   iron,  (int)caffeine AS caffeine;

STORE drinks_extended_converted INTO 'starbucks-clean-data/clean-drinks-extended' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
