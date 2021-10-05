-- Task 1
select c.name, count(f.film_id) films

from category c
join film_category fc on c.category_id = fc.category_id
join film f on fc.film_id = f.film_id

group by c.category_id

order by films desc;


-- Task 2
select a.first_name, a.last_name, count(r.rental_id) rentals

from actor a
join film_actor fa on a.actor_id = fa.actor_id
join film f on fa.film_id = f.film_id
join inventory i on f.film_id = i.film_id
join rental r on i.inventory_id = r.inventory_id

group by a.actor_id

order by rentals desc

limit 10;


-- Task 3
select c.name, sum(p.amount) rental_sum

from category c
join film_category fc on c.category_id = fc.category_id
join film f on fc.film_id = f.film_id
join inventory i on f.film_id = i.film_id
join rental r on i.inventory_id = r.inventory_id
join payment p on p.rental_id = r.rental_id

group by c.category_id

order by rental_sum desc

limit 1;

-- Task 4
select f.title

from film f
left join inventory i on f.film_id = i.film_id

where i.inventory_id is null

group by f.film_id;

-- или так, тоже ведь без IN =)
select f.title

from film f

where not exists (select film_id
                  from inventory i
                  where f.film_id = i.film_id);

-- Task 5
select a.first_name, a.last_name, films from (

select unnest(actors) actor_id, films from (

select films, array_agg(actor_id) actors from (

select fa.actor_id, count(f.film_id) films

from category c
join film_category fc on c.category_id = fc.category_id
join film f on fc.film_id = f.film_id
join film_actor fa on fa.film_id = f.film_id


where c.category_id = 3

group by fa.actor_id

order by films desc

) child_films_actors

group by 1
order by 1 desc
limit 3

) child_films_actors_limited

) actors_films

join actor a on a.actor_id = actors_films.actor_id

-- Task 6
select ci.city
     , count(cu.active = 1 or null) AS active
     , count(cu.active = 0 or null) AS inactive

from customer cu
left join address ad on ad.address_id = cu.address_id
left join city ci on ad.city_id = ci.city_id

group by ci.city_id

order by inactive desc

-- Task 7
with

     city_category as (

         select city_id, category_id

         from (
                  select city_id,
                         first_value(category_id)
                         over (PARTITION BY city_id order by max(return_hourdiff) desc) category_id

                  from (
                           select ad.city_id,
                                  fc.category_id,
                                  sum(DATE_PART('day', return_date - rental_date) * 24 +
                                      DATE_PART('hour', return_date - rental_date)) return_hourdiff

                           from film f
                                    join film_category fc on fc.film_id = f.film_id
                                    join inventory i on f.film_id = i.film_id
                                    join rental r on i.inventory_id = r.inventory_id
                                    join customer cu on cu.customer_id = r.customer_id
                                    left join address ad on ad.address_id = cu.address_id

                           group by ad.city_id, fc.category_id
                       ) cities
                  group by city_id, category_id
              ) cities
         group by 1, 2
     )

select co.country, city.city, cat.name from city_category

join city on city_category.city_id = city.city_id
join country co on co.country_id = city.country_id
join category cat on city_category.category_id = cat.category_id

where city.city ilike 'a%'

union

select co.country, city.city, cat.name from city_category

join city on city_category.city_id = city.city_id
join country co on co.country_id = city.country_id
join category cat on city_category.category_id = cat.category_id

where city.city like '%-%'
