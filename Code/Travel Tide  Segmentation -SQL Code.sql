Travel Tide SQL Code


-- Create a temporary table based on sessions data with detailed user and trip information
WITH session_based AS (
    SELECT 
        s.session_id,
        s.user_id,
        s.trip_id,
        s.session_start,
        s.session_end,
        s.page_clicks,
        s.flight_discount,
        s.flight_discount_amount,
        s.hotel_discount,
        s.hotel_discount_amount,
        s.flight_booked,
        s.hotel_booked,
        s.cancellation,
        -- Calculate session duration in minutes
        EXTRACT(EPOCH FROM (s.session_end - s.session_start)) / 60 AS session_duration_mins,
        -- Join with flight information
        f.origin_airport,
        f.destination,
        f.destination_airport,
        f.seats,
        f.return_flight_booked,
        f.departure_time,
        f.return_time,
        f.checked_bags,
        f.trip_airline,
        f.destination_airport_lat,
        f.destination_airport_lon,
        f.base_fare_usd,
        -- Join with hotel information
        h.hotel_name,
        CASE WHEN h.nights <= 0 THEN 1 ELSE h.nights END AS nights, -- Ensure no negative/zero values for nights
        h.rooms,
        h.check_in_time,
        h.check_out_time,
        h.hotel_per_room_usd AS hotel_price_per_room_night_usd,
        -- Join with user data
        u.has_children, u.married, u.gender, u.birthdate, u.home_country, u.home_city, u.home_airport,
        u.home_airport_lat,
        u.home_airport_lon
    FROM sessions s
    LEFT JOIN users u ON s.user_id = u.user_id -- Join user information
    LEFT JOIN flights f ON s.trip_id = f.trip_id -- Join flight information
    LEFT JOIN hotels h ON s.trip_id = h.trip_id -- Join hotel information
    -- Filter for users active after a specific date and with more than 7 sessions
    WHERE s.user_id IN (
        SELECT s.user_id
        FROM sessions s
        WHERE s.session_start > '2023-01-04'
        GROUP BY s.user_id
        HAVING COUNT(*) > 7
    )
),


-- Aggregate session-level data to user level
session_user_based AS (
    SELECT 
        sb.user_id,
        -- Count rooms and nights
        COUNT(rooms) AS num_rooms,
        COUNT(nights) AS num_nights,
        -- Count total clicks and sessions
        SUM(sb.page_clicks) AS num_clicks,
        COUNT(DISTINCT sb.session_id) AS num_sessions,
        -- Calculate average session duration
        ROUND(AVG(sb.session_duration_mins), 2) AS avg_session_duration_mins,
        -- Count total flight and hotel bookings
        SUM(CASE WHEN sb.flight_booked THEN 1 ELSE 0 END) AS total_flight_bookings,
        SUM(CASE WHEN sb.hotel_booked THEN 1 ELSE 0 END) AS total_hotel_bookings,
        -- Calculate average discount percentages
        ROUND(COALESCE(AVG(sb.flight_discount_amount), 0), 2) AS avg_flight_discount_percent,
        ROUND(COALESCE(AVG(sb.hotel_discount_amount), 0), 2) AS avg_hotel_discount_percent,
        -- Count cancellations and calculate proportions
        SUM(CASE WHEN sb.cancellation THEN 1 ELSE 0 END) AS total_cancellations,
        -- Calculate booking rate and cancellation rate
        CASE WHEN COUNT(sb.trip_id) > 0 THEN (SUM(CASE WHEN flight_booked IS TRUE AND hotel_booked IS TRUE THEN 1 ELSE 0 END)::FLOAT /
                                                           SUM(CASE WHEN flight_booked IS TRUE OR hotel_booked IS TRUE THEN 1 ELSE 0 END)) ELSE NULL END AS booking_rate,
        CASE WHEN COUNT(sb.trip_id) > 0 THEN (SUM(CASE WHEN cancellation IS TRUE THEN 1 ELSE 0 END)::FLOAT /
                                                           SUM(CASE WHEN flight_booked IS TRUE OR hotel_booked IS TRUE THEN 1 ELSE 0 END)) ELSE NULL END AS cancellation_rate,
        -- Proportion of sessions with discounts
        SUM(CASE WHEN sb.flight_discount THEN 1 ELSE 0 END) :: FLOAT / COUNT(*) AS discount_flight_proportion,
        ROUND(AVG(sb.flight_discount_amount), 2) AS average_flight_discount,
                -- Aggregate spending and calculate distance-normalized spend (ADS)
        SUM(sb.flight_discount_amount * sb.base_fare_usd) / 
            SUM(haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)) AS ADS,
        SUM(CASE WHEN sb.hotel_discount THEN 1 ELSE 0 END) :: FLOAT / COUNT(*) AS discount_hotel_proportion,
        ROUND(AVG(sb.hotel_discount_amount), 2) AS average_hotel_discount,
                CASE WHEN SUM(sb.nights) > 0 THEN (SUM(sb.hotel_price_per_room_night_usd * sb.rooms * sb.hotel_discount_amount)/SUM(sb.nights)) 
             ELSE NULL END AS ADS_night,
        -- Calculate average number of checked bags
        SUM(sb.checked_bags)::FLOAT / COUNT(*) AS avg_bags,
        -- Categorize activity types based on booking patterns
        CASE
            WHEN SUM(CASE WHEN sb.flight_booked THEN 1 ELSE 0 END) > 0 AND SUM(CASE WHEN sb.hotel_booked THEN 1 ELSE 0 END) = 0 THEN 'Flight Only'
            WHEN SUM(CASE WHEN sb.flight_booked THEN 1 ELSE 0 END) > 0 AND SUM(CASE WHEN sb.hotel_booked THEN 1 ELSE 0 END) > 0 THEN 'Flight with Hotel'
            WHEN SUM(CASE WHEN sb.flight_booked THEN 1 ELSE 0 END) = 0 AND SUM(CASE WHEN sb.hotel_booked THEN 1 ELSE 0 END) > 0 THEN 'Hotel Only'
            ELSE 'No Activity'
        END AS activity_type,
        -- Scale session duration
        ROUND((AVG(sb.session_duration_mins) - MIN(AVG(sb.session_duration_mins)) OVER()) 
            / NULLIF((MAX(AVG(sb.session_duration_mins)) OVER() - MIN(AVG(sb.session_duration_mins)) OVER()), 0), 2) AS scaled_session_duration
    FROM session_based sb
    GROUP BY sb.user_id
),
trip_based AS (
    SELECT 
        sb.user_id,
        -- Total number of trips
        COUNT(sb.trip_id) AS num_trips,
        -- Total checked bags
        COALESCE(SUM(sb.checked_bags), 0) AS total_checked_bags,
        -- Average spending per trip (flights + hotels)
        AVG((base_fare_usd * seats) + (hotel_price_per_room_night_usd * rooms * nights)) AS avg_amount_spent,
        -- Total spending on hotels and flights, accounting for discounts
        ROUND(COALESCE(SUM((sb.hotel_price_per_room_night_usd * sb.nights * sb.rooms) * (1 - COALESCE(sb.hotel_discount_amount, 0))), 0), 2) AS money_spent_hotel,
        ROUND(COALESCE(SUM((sb.base_fare_usd * sb.seats) * (1 - COALESCE(sb.flight_discount_amount, 0))), 0), 2) AS money_spent_flight,
        -- Average hotel price per room per night
        ROUND(COALESCE(AVG(sb.hotel_price_per_room_night_usd), 0), 2) AS avg_hotel_price_per_room_night_usd,
        -- Average flight distance in kilometers
        ROUND(COALESCE(AVG(haversine_distance(sb.home_airport_lat, sb.home_airport_lon, sb.destination_airport_lat, sb.destination_airport_lon))::numeric, 0), 2) AS avg_km_flown,
        -- Normalized base fare for scaling purposes
        ROUND(COALESCE(AVG(sb.base_fare_usd), 0), 2) AS scaled_fare_usd
    FROM session_based sb 
    JOIN session_user_based sub ON sb.user_id = sub.user_id -- Ensure alignment with user-level data
    GROUP BY sb.user_id
),
score_table AS (
    SELECT sb.user_id, 
        -- Free hotel meals: higher score for family travelers or senior travelers
        ROUND(SUM((CASE WHEN rooms >= 2 THEN 0.5 ELSE 0 END) +
                  (CASE WHEN has_children = true THEN 0.3 ELSE 0 END) +
                  (CASE WHEN EXTRACT(YEAR FROM age(now(), birthdate)) >= 56 THEN 0.2 ELSE 0 END)) / COUNT(*), 2) AS score_free_hotel_meals,
        -- Free checked bags: higher score for users booking multiple bags or long flights
        ROUND(SUM((CASE WHEN checked_bags >= 2 THEN 0.7 ELSE 0 END) +
                  (CASE WHEN (haversine_distance(home_airport_lat, home_airport_lon, 
                                                 destination_airport_lat, destination_airport_lon) * 6371) > 1000 THEN 0.3 ELSE 0 END)) / COUNT(*), 2) AS score_free_checked_bag,
        -- No cancellation fee: higher score for frequent cancellations or complete bookings
        ROUND(SUM((CASE WHEN cancellation = true THEN 0.5 ELSE 0 END) +
                  (CASE WHEN flight_booked = true AND hotel_booked = true THEN 0.5 ELSE 0 END)) / COUNT(*), 2) AS score_no_cancellation_fee,
        -- One-night free with flight: higher score for short hotel stays with flights
        ROUND(SUM((CASE WHEN nights < 2 THEN 0.5 ELSE 0 END) +
                  (CASE WHEN flight_booked = true AND hotel_booked = true THEN 0.5 ELSE 0 END)) / COUNT(*), 2) AS score_1_night_free_with_flight,
        -- Exclusive discount: higher score for high spenders or users frequently utilizing discounts
        ROUND(SUM((CASE WHEN avg_amount_spent > 1000 THEN 0.5 ELSE 0 END) +
                  (CASE WHEN flight_discount = true OR hotel_discount = true THEN 0.5 ELSE 0 END)) / COUNT(*), 2) AS score_exclusive_discount
    FROM session_based sb
    JOIN trip_based tb ON sb.user_id = tb.user_id -- Align trip and session data
    GROUP BY sb.user_id
),
ranked_table AS (
    SELECT user_id,
           -- Free hotel meals ranking
           score_free_hotel_meals,
           RANK() OVER (ORDER BY score_free_hotel_meals DESC) AS rank_free_hotel_meals,
           -- Free checked bag ranking
           score_free_checked_bag,
           RANK() OVER (ORDER BY score_free_checked_bag DESC) AS rank_free_checked_bag,
           -- No cancellation fee ranking
           score_no_cancellation_fee,
           RANK() OVER (ORDER BY score_no_cancellation_fee DESC) AS rank_no_cancellation_fee,
           -- One night free with flight ranking
           score_1_night_free_with_flight,
           RANK() OVER (ORDER BY score_1_night_free_with_flight DESC) AS rank_1_night_free_with_flight,
           -- Exclusive discount ranking
           score_exclusive_discount,
           RANK() OVER (ORDER BY score_exclusive_discount DESC) AS rank_exclusive_discount
    FROM score_table
),
final_code AS (
    SELECT sb.user_id,
           -- Basic user demographics and details
           u.gender, 
           EXTRACT(YEAR FROM age(now(), u.birthdate)) AS age, 
           u.married, 
           u.has_children, 
           u.home_country, 
           u.home_city, 
           u.home_airport, 
           u.sign_up_date,
           -- Session-based and trip-based metrics
           sb.num_clicks, sb.num_sessions, sb.avg_session_duration_mins, sb.total_flight_bookings,
           sb.total_hotel_bookings, sb.avg_flight_discount_percent, sb.avg_hotel_discount_percent, 
           sb.discount_flight_proportion, sb.discount_hotel_proportion, sb.total_cancellations, sb.activity_type, 
           sb.scaled_session_duration, sb.booking_rate, sb.cancellation_rate, sb.num_nights, sb.num_rooms,
           tb.num_trips, tb.total_checked_bags, tb.avg_amount_spent, tb.money_spent_hotel, tb.money_spent_flight,
           tb.avg_hotel_price_per_room_night_usd, tb.avg_km_flown, tb.scaled_fare_usd,
           -- Traveler profile classification
           CASE  
              WHEN EXTRACT(YEAR FROM age(now(), u.birthdate)) > 55 THEN 'senior traveller'
              WHEN has_children THEN 'family travellers'
              WHEN EXTRACT(YEAR FROM age(now(), u.birthdate)) < 35 AND tb.num_trips < 2 THEN 'dreamer traveller'
              WHEN EXTRACT(YEAR FROM age(now(), u.birthdate)) < 35 AND tb.num_trips >= 2 THEN 'young frequent traveller'
              WHEN EXTRACT(YEAR FROM age(now(), u.birthdate)) >= 35 AND tb.num_trips > 5 THEN 'business traveller'
              ELSE 'Normal traveller'
           END AS traveler_profile,
           -- Perk assignment based on the lowest rank for each score
           CASE
              WHEN rank_free_hotel_meals < rank_free_checked_bag 
                   AND rank_free_hotel_meals < rank_no_cancellation_fee
                   AND rank_free_hotel_meals < rank_1_night_free_with_flight
                   AND rank_free_hotel_meals < rank_exclusive_discount
              THEN 'Free Hotel Meals'
              WHEN rank_free_checked_bag < rank_free_hotel_meals
                   AND rank_free_checked_bag < rank_no_cancellation_fee
                   AND rank_free_checked_bag < rank_1_night_free_with_flight
                   AND rank_free_checked_bag < rank_exclusive_discount
              THEN 'Free Checked Bag'
              WHEN rank_no_cancellation_fee < rank_free_hotel_meals
                   AND rank_no_cancellation_fee < rank_free_checked_bag
                   AND rank_no_cancellation_fee < rank_1_night_free_with_flight
                   AND rank_no_cancellation_fee < rank_exclusive_discount
              THEN 'No Cancellation Fee'
              WHEN rank_1_night_free_with_flight < rank_free_hotel_meals
                   AND rank_1_night_free_with_flight < rank_free_checked_bag
                   AND rank_1_night_free_with_flight < rank_no_cancellation_fee
                   AND rank_1_night_free_with_flight < rank_exclusive_discount
              THEN '1 Night Free With Hotel'
              ELSE 'Exclusive Discount'
           END AS perk_assignment
    FROM ranked_table rt
    JOIN session_user_based sb ON rt.user_id = sb.user_id
    JOIN trip_based tb ON sb.user_id = tb.user_id
    JOIN users u ON sb.user_id = u.user_id
)
SELECT * FROM final_code;
