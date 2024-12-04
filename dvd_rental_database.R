install.packages("RPostgres")

# Create a connection object
con <- dbConnect(
  Postgres(),
  dbname = "Jeff",
  host = Sys.getenv("PG_HOST"),
  port = Sys.getenv("PG_PORT"),
  user = Sys.getenv("PG_USR"),
  password = Sys.getenv("PG_PASS")
)

Sys.setenv(PG_HOST = "mathmads.canterbury.ac.nz")
Sys.setenv(PG_PORT = "8909")
Sys.setenv(PG_USR = "student_data422")
Sys.setenv(PG_PASS = "readonly")

usethis::edit_r_environ()

library(DBI)
library(RPostgres)

# Establish the connection
con <- dbConnect(
  Postgres(),
  dbname = "Jeff",
  host = Sys.getenv("PG_HOST"),
  port = Sys.getenv("PG_PORT"),
  user = Sys.getenv("PG_USR"),
  password = Sys.getenv("PG_PASS")
)

# Check if the connection is valid
dbIsValid(con)


# List all tables in DataBase

dbListTables(con)

# List all fields (columns) in specific table

dbListFields(con, "rental")

# Pull Data from table (first 10 rows)

dbGetQuery(con, "SELECT * FROM rental LIMIT 10")

# Count total number of rentals

query <- "SELECT COUNT(*) FROM rental"
dbGetQuery(con, query)

# Start plotting data


# Define the query
query <- "
  SELECT DATE(payment_date) AS payment_day, 
         SUM(amount) AS total_revenue
  FROM payment
  GROUP BY DATE(payment_date)
  ORDER BY payment_day;
"

# Run the query and store the result in a dataframe
revenue_data <- dbGetQuery(con, query)

# Print data
print(revenue_data)

library(ggplot2)

# Create a line plot
ggplot(revenue_data, aes(x = payment_day, y = total_revenue)) +
  geom_line() +
  labs(
    title = "Total Daily Rental Revenue",
    x = "Date",
    y = "Total Revenue"
  ) +
  theme_minimal()


# Joining across tables

query <- "
  SELECT 
    customer.first_name || ' ' || customer.last_name AS customer_name,
    film.title AS film_title,
    DATE(rental.rental_date) AS rental_date,  -- Extract only the date part
    payment.amount AS total_payment
  FROM rental
  INNER JOIN customer ON rental.customer_id = customer.customer_id
  INNER JOIN payment ON rental.rental_id = payment.rental_id
  INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
  INNER JOIN film ON inventory.film_id = film.film_id
  ORDER BY rental.rental_date;
"



rental_details <- dbGetQuery(con, query)

head(rental_details)


# Inventory Stock take

query <- "
WITH r_rated_movies AS (
    SELECT 
        film.film_id, 
        film.title, 
        category.name AS category_name, 
        store.store_id, 
        rental.staff_id,  -- Staff information from the rental table
        COUNT(inventory.inventory_id) AS dvd_count
    FROM film
    INNER JOIN film_category ON film.film_id = film_category.film_id
    INNER JOIN category ON film_category.category_id = category.category_id
    INNER JOIN inventory ON film.film_id = inventory.film_id
    INNER JOIN store ON inventory.store_id = store.store_id
    INNER JOIN rental ON rental.inventory_id = inventory.inventory_id  -- Fifth join: join rental to inventory
    WHERE film.rating = 'R'  -- Only R-rated movies
    GROUP BY store.store_id, category.name, film.film_id, rental.staff_id
)
SELECT 
    store_id, 
    category_name, 
    SUM(dvd_count) AS total_dvds
FROM r_rated_movies
GROUP BY store_id, category_name
ORDER BY store_id, category_name;
"

# Execute the query
r_rated_data <- dbGetQuery(con, query)

# View the first few rows to confirm the data
head(r_rated_data)

# Load ggplot2 for plotting
library(ggplot2)

# Plot the results as a bar chart comparing the two stores
ggplot(r_rated_data, aes(x = factor(store_id), y = total_dvds, fill = category_name)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(
    title = "R-Rated Movie Stock by Store",
    x = "Store ID",
    y = "Total DVDs",
    fill = "Movie Category"
  ) +
  theme_minimal()


# Optimization

# Add explain analysis to the query

query <- "
EXPLAIN ANALYZE
WITH r_rated_movies AS (
    SELECT 
        film.film_id, 
        film.title, 
        category.name AS category_name, 
        store.store_id, 
        rental.staff_id,  -- Staff information from the rental table
        COUNT(inventory.inventory_id) AS dvd_count
    FROM film
    INNER JOIN film_category ON film.film_id = film_category.film_id
    INNER JOIN category ON film_category.category_id = category.category_id
    INNER JOIN inventory ON film.film_id = inventory.film_id
    INNER JOIN store ON inventory.store_id = store.store_id
    INNER JOIN rental ON rental.inventory_id = inventory.inventory_id  -- Fifth join
    WHERE film.rating = 'R'
    GROUP BY store.store_id, category.name, film.film_id, rental.staff_id
)
SELECT 
    store_id, 
    category_name, 
    SUM(dvd_count) AS total_dvds
FROM r_rated_movies
GROUP BY store_id, category_name
ORDER BY store_id, category_name;
"

# Run the EXPLAIN ANALYZE query
explain_output <- dbGetQuery(con, query)

# View the output
print(explain_output)


# Question 01

# Which step to Optimize first?

# Seq Scans on rental and inventory. The sequential scans on the rental and inventory tables are potential areas for optimization. 
# Sequential scans are fine on smaller data sets but can slow down queries on larger data sets.
# As optimization strategy we should consider adding an index on columns that are frequently queried such as rental.inventory_id
# and inventory.film_id.
# This reduce the time it takes to perform lookups and avoid scanning the entire table.


# Question 02 

# What does that step do?

# A sequential scans reads all the rows in a table sequentially, which can be inefficient for large tables.
# It doesn't make use of indexes to retrieve only the relevant rows,
# and as the table grows, the time taken for this step increases. 
# Index Scan - By creating index, PostgreSQL can perform an index scan, which only scans the relevant rows of a query,
# making it much faster on larger data sets. 

dbDisconnect(con)





