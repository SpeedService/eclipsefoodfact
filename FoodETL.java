package foodartifact;

import java.lang.reflect.Array;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.collection.immutable.Seq;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class FoodETL {

	public static void showColumnInfo(String column_name, Dataset<Row> df) {
		Dataset<Row> uniqueValues = df.select(column_name).distinct();
		long countNonNull = df.filter(df.col(column_name).isNotNull()).count();
		uniqueValues.show(100, false);

		System.out.println("Nombre de lignes total : " + countNonNull);
		System.out.println("Nombre de lignes uniques : " + uniqueValues.count());
	}

	// Récupérer les bonnes colonnes pour le regime sans gluten
	public static Dataset<Row> getGlutenColumns(Dataset<Row> df) {

		Dataset<Row> dataset = df.select(
				df.col("product_name").cast("string"),
				// df.col("quantity").cast("string"),
				df.col("countries_en").cast("string"),
				df.col("food_groups").cast("string"),
				df.col("traces_en").cast("string"), 
				df.col("ingredients_tags").cast("string"),
				df.col("ingredients_analysis_tags").cast("string"),
				df.col("nutrient_levels_tags").cast("string"),
				df.col("additives_en").cast("string"));

		dataset = dataset.na().drop();

		return dataset;

	}

	// Récupérer les columns filtrées des mauvaises données.
	public static Dataset<Row> formatColumnsGluten(Dataset<Row> df) {

		Dataset<Row> formated_df = df;

		// Retirer les lignes qui contiennent "gluten"
		for (String columnName : formated_df.columns()) 
			formated_df = formated_df.filter(not(lower(formated_df.col(columnName)).contains("gluten")));
		
		
		// Prendre que les produits qui sont en France.
		formated_df = formated_df.filter(lower(formated_df.col("countries_en")).contains("france"));

		// Rendre les données plus lisible en enlevant les en: fr: sw: gr: ...
		for (String columnName : formated_df.columns()) 
			formated_df = formated_df.withColumn(columnName, regexp_replace(col(columnName), ".{2}:", ""));
		

		return formated_df;
	}
	
	// Obtenir une liste avec des catégories de nourritures
	public static Map<String, List<String>> getFoodCategoriesByLunchTime() {
		
		// Faire la list des group de nourriture pour matin midi et soir
		Map<String, List<String>> foodCategories = new HashMap<>();

        // Creating lists for breakfast, lunch, and dinner
        List<String> breakfastList = new ArrayList<>();
        List<String> mealList = new ArrayList<>();
        List<String> dessertList = new ArrayList<>();
        
        // Types pour le plats
        List<String> fishList = new ArrayList<>();
        List<String> meatList = new ArrayList<>();
        List<String> vegetableList = new ArrayList<>();
        List<String> starchyList = new ArrayList<>(); // feculents


        // Ajouter a la liste des petits dejeuner
        breakfastList.add("sweets");
        breakfastList.add("cereals");
        breakfastList.add("biscuits-and-cakes");
        breakfastList.add("pastries");
        breakfastList.add("chocolate-products");

        // Ajouter à la list des plat
        mealList.add("sandwiches");
        mealList.add("soups");
        mealList.add("one-dish-meals");
        mealList.add("appetizers");
        mealList.add("poultry");
        mealList.add("pizza-pies-and-quiches");
        mealList.add("legumes");
        mealList.add("processed-meat");
        mealList.add("fish-and-seafood");
        mealList.add("offals");
        mealList.add("fats");
        mealList.add("potatoes");
        mealList.add("fatty-fish");
        mealList.add("cereals-and-potatoes");
        mealList.add("lean-fish");
        mealList.add("eggs");
        mealList.add("fish-meat-eggs");
        
        // Ajouter a la list des desserts
        dessertList.add("cheese");
        dessertList.add("nuts");
        dessertList.add("fruits-and-vegetables");
        dessertList.add("fruits");
        dessertList.add("dairy-desserts");
        dessertList.add("ice-cream");
        dessertList.add("dried-fruits");
        dessertList.add("milk-and-yogurt");
        
        // Ajouter à la liste des poissons
        fishList.add("fish-and-seafood");
        fishList.add("fatty-fish");
        fishList.add("lean-fish");
        fishList.add("fats");
        fishList.add("fish-meat-eggs");
        fishList.add("one-dish-meals");
        
        // Ajouter à la liste des viandes
        meatList.add("appetizers");
        meatList.add("poultry");
        meatList.add("pizza-pies-and-quiches");
        meatList.add("poultry");
        meatList.add("processed-meat");
        meatList.add("offals");
        meatList.add("fish-meat-eggs");
        meatList.add("one-dish-meals");
        
        // Ajouter à la list des legumes
        vegetableList.add("legumes");
        vegetableList.add("vegetables");
        
        // Ajouter à la list des feculent
        starchyList.add("soups");
        starchyList.add("cereals-and-potatoes");
        starchyList.add("eggs");

        // Putting the lists into the map
        foodCategories.put("breakfast", breakfastList);
        foodCategories.put("meal", mealList);
        foodCategories.put("dessert", dessertList);
        foodCategories.put("fish", fishList);
        foodCategories.put("meat", meatList);
        foodCategories.put("starchy", starchyList);
        foodCategories.put("vegetable", vegetableList);
  
        // System.out.println("Breakfast: " + foodCategories.get("breakfast"));

		return foodCategories;
	}
	
	// Renvoie un produit aléatoire, de la categories voulu.
	public static List<String> getRandomProductsByFoodCategories(
			Dataset<Row> df,
			String category, 
			int numberOfRandomProducts ){
		
		System.out.println("Category : " +category);
		Map<String, List<String>> food_categories = getFoodCategoriesByLunchTime();
		List<String> breakfast_list = food_categories.get(category);
		
        // Recuperer les lignes qui ont les groupes definit dans category
        Dataset<Row> filtered_df = df.filter(
        		df.col("food_groups").isin(breakfast_list.toArray())
        		);
        
        List<String> productsList = new ArrayList<>();
        
        // Convertir la dataframe en javaRDD
        JavaRDD<Object> columnValuesRDD = filtered_df.select("product_name").javaRDD().map(row -> row.get(0));
        
        // Récuperer le nombre de produit définit en parametre
        for (int i = 0; i < numberOfRandomProducts; i++) {
            Object valueAtIndex = columnValuesRDD.take(i + 1).get(i);
            productsList.add(valueAtIndex.toString());
		}
       
        
		return productsList;
	}

	// Retourne 2 produit de la categorie breakfast
	public static Map<String, List<String>> retrieveBreakfast(Dataset<Row> df){
		
		List<String> listBreakfastRandom = getRandomProductsByFoodCategories(df,"breakfast",2);
		
		Map<String, List<String>> breakfastDict = new HashMap<>();
		breakfastDict.put("breakfast", listBreakfastRandom);
		
		return breakfastDict;
	}
		
	// Retourne 1 plat et 1 dessert = retourne un repas.
	public static Map<String, List<String>> retrieveMeal(Dataset<Row> df){
		df = df.limit(250).orderBy(rand());
		
		Map<String, List<String>> mealDict = new HashMap<>();
		
		List<String> listRandomDessert = getRandomProductsByFoodCategories(df,"dessert",1);

        Random randomInt = new Random();
        String randomFat = "";
        String randomStarch = "";
        
        if (randomInt.nextInt(2) == 1) {
        	 randomStarch = getRandomProductsByFoodCategories(df,"vegetable",1).toString();
		}else {
			 randomStarch = getRandomProductsByFoodCategories(df,"starchy",1).toString();
		}
        
        if (randomInt.nextInt(2) == 1) {
        	 randomFat = getRandomProductsByFoodCategories(df,"fish",1).toString();
		}else {
			 randomFat = getRandomProductsByFoodCategories(df,"meat",1).toString();
		}
        
        List<String> listRandomMeal = new ArrayList<>();
       
        listRandomMeal.add(randomStarch);
        listRandomMeal.add(randomFat);
        
        mealDict.put("meal", listRandomMeal);
        mealDict.put("dessert", listRandomDessert);
        
		return mealDict;
	}
	
	// Utilises les fonctions précedentes pour obtenir une liste avec petit-deheuner, dejeuner, diner.
	public static void generateDays(Dataset<Row> df,int daysToGenerate){
		
		Map<String,List<Map<String, List<String>>>> mealsEachDay = new HashMap<> ();		

		Map<String, List<String>> dinner = new HashMap<> ();
		Map<String, List<String>> lunch = new HashMap<> ();
		Map<String, List<String>> breakfast = new HashMap<> ();
		
		List<Map<String, List<String>>> dayMeals = new ArrayList<>();
		
	
		
		for (int i = 1; i <= daysToGenerate; i++) {
			dayMeals.clear();
			
			// Melanger le dataset
			
			breakfast = retrieveBreakfast(df);
			lunch = retrieveMeal(df);
			dinner = retrieveMeal(df);
			
			dayMeals.add(breakfast);
			dayMeals.add(lunch);
			dayMeals.add(dinner);
			
			
			mealsEachDay.put("Jour "+i, dayMeals);
		}
			
		// Affichage de la liste mealsEachDay dans la console
		for (Map.Entry<String, List<Map<String, List<String>>>> entry : mealsEachDay.entrySet()) {
			System.out.println();
            System.out.println("Pour le " + entry.getKey());
            List<Map<String, List<String>>> innerList = entry.getValue();
            
            for (Map<String, List<String>> innerMap : innerList) {
                for (Map.Entry<String, List<String>> innerEntry : innerMap.entrySet()) {
                	System.out.println();
                    System.out.println("  Repas: " + innerEntry.getKey());
                    
                    List<String> innerValue = innerEntry.getValue();
                    
                    for (String value : innerValue) {
                        System.out.println(value);
                    }
                }
            }
            System.out.println();
        }
	
	}
	

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().appName("Food Fact").master("local[*]").getOrCreate();

		String date_source_path = "D:/Cours/integrationDonnee/food.csv";

		Dataset<Row> df = sparkSession.read().format("csv").option("header", "true").option("delimiter", "\t")
				.load(date_source_path);

		
		// Regime sans gluten
		Dataset<Row> dataset_gluten = getGlutenColumns(df); // recuperer les bonnes colonnes
		dataset_gluten = formatColumnsGluten(dataset_gluten); // filtre les colonnes
		dataset_gluten = dataset_gluten.orderBy(rand()).limit(1000); // mélange le dataset
			
		generateDays(dataset_gluten,1); // générer un repas pour 1 jour avec le régime dans gluten
		
		
		
		
		sparkSession.stop();
	}

}
