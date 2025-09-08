-- database creation schema
create database social_media;
use social_media;

-- Section 1: Data Cleaning and Schema Design
-- We will normalize the flat, un-cleaned dataset into four tables:
-- Users, Posts, Likes, and Comments.

-- 1. Create the Users table
-- This table will store unique user information.
CREATE TABLE Users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);

-- 2. Create the Posts table
-- This table will store information about each post.
CREATE TABLE Posts (
    post_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    post_content TEXT,
    post_date TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

-- 3. Create the Likes table
-- This table tracks each individual 'like' action on a post.
CREATE TABLE Likes (
    like_id SERIAL PRIMARY KEY,
    post_id INT NOT NULL,
    user_id INT NOT NULL,
    like_date TIMESTAMP NOT NULL,
    FOREIGN KEY (post_id) REFERENCES Posts(post_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

-- 4. Create the Comments table
-- This table stores individual comments.
CREATE TABLE Comments (
    comment_id SERIAL PRIMARY KEY,
    post_id INT NOT NULL,
    user_id INT NOT NULL,
    comment_content TEXT NOT NULL,
    comment_date TIMESTAMP NOT NULL,
    FOREIGN KEY (post_id) REFERENCES Posts(post_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

-- Note: The `likes` and `comments` columns from the original dataset
-- represent a *count*, not individual actions. To make the project
-- more realistic and follow a proper relational design, we will populate
-- the Likes and Comments tables with a reasonable number of "dummy" entries
-- for each post, based on the original counts. This simulates real user activity.

-- Section 2: Populating Data with Realistic Activities
-- This is a one-time process to load the initial data.
-- Normally, an application would insert data one by one.

-- We'll use a temporary table to simulate the un-cleaned data for insertion.
CREATE TEMPORARY TABLE RawData (
    user_id INT,
    username VARCHAR(50),
    email VARCHAR(100),
    post_id INT,
    post_content TEXT,
    post_date TIMESTAMP,
    likes INT,
    comments INT
);

-- The following INSERT statements are based on the CSV data provided.
-- You would run these to load the data into the temporary table.
-- I'll insert a few rows as an example, since listing all 1000+ would be very long.
-- You can generate the rest programmatically or use a data loading tool.

INSERT INTO RawData (user_id, username, email, post_id, post_content, post_date, likes, comments) VALUES
(1, 'johndoe', 'john@example.com', 101, 'Just a test post.', '2023-01-15 10:30:00', 5, 3),
(2, 'marysmith', 'mary.smith@mail.co', 102, 'My first post here!', '2023-01-15 11:00:00', 12, 5),
(3, 'ali_g', 'alig@email.net', 103, 'Feeling great today!', '2023-01-15 12:00:00', 9, 2),
(4, 'sarah_p', 'sarahp@gmail.com', 104, 'Lunch time!', '2023-01-15 13:00:00', 15, 7),
(5, 'chrisW', 'chris.w@yahoo.com', 105, 'Hello world!', '2023-01-15 14:00:00', 10, 4),
(6, 'emily_jones', 'emily.j@web.de', 106, 'Coding all night.', '2023-01-15 15:00:00', 7, 1),
(7, 'alex123', 'alex@example.net', 107, 'New profile pic!', '2023-01-15 16:00:00', 18, 6),
(8, 'brian_k', 'briank@mail.com', 108, 'Loving this weather.', '2023-01-15 17:00:00', 22, 11),
(9, 'linda_b', 'linda@b.net', 109, 'What a day...', '2023-01-15 18:00:00', 6, 3),
(10, 'steve_x', 'stevex@example.org', 110, 'Thoughts?', '2023-01-15 19:00:00', 14, 5);
-- ... and so on for all 100 records ...

-- Now, we'll insert data from RawData into our clean tables,
-- handling duplicates and creating separate entries for likes/comments.

-- Insert unique users, ignoring conflicts on username/email
INSERT INTO Users (user_id, username, email)
SELECT DISTINCT user_id, username, email
FROM RawData
ON CONFLICT (user_id) DO NOTHING;

-- Insert posts
INSERT INTO Posts (post_id, user_id, post_content, post_date)
SELECT post_id, user_id, post_content, post_date
FROM RawData
ON CONFLICT (post_id) DO NOTHING;

-- Insert individual likes and comments for each post to simulate activity.
-- We use a GENERATE_SERIES function to create one row for each 'like' or 'comment' count.
-- This is a key step to "clean" the summarized data into relational data.
INSERT INTO Likes (post_id, user_id, like_date)
SELECT T.post_id, T.user_id, T.post_date + (INTERVAL '1 second' * s.i)
FROM RawData AS T
CROSS JOIN LATERAL GENERATE_SERIES(1, T.likes) AS s(i);

INSERT INTO Comments (post_id, user_id, comment_content, comment_date)
SELECT T.post_id, T.user_id, 'This is comment ' || s.i, T.post_date + (INTERVAL '1 second' * s.i)
FROM RawData AS T
CROSS JOIN LATERAL GENERATE_SERIES(1, T.comments) AS s(i);


-- Section 3: Project Deliverables - Analytics and Reports
-- Now that the data is clean and structured, we can perform the analytics.

-- 1. Create a View for Top Posts (based on total likes and comments)
-- This view will provide quick access to key post engagement data.
CREATE VIEW PostEngagement AS
SELECT
    p.post_id,
    p.post_content,
    p.post_date,
    u.username,
    u.email,
    COUNT(DISTINCT l.like_id) AS total_likes,
    COUNT(DISTINCT c.comment_id) AS total_comments
FROM Posts p
JOIN Users u ON p.user_id = u.user_id
LEFT JOIN Likes l ON p.post_id = l.post_id
LEFT JOIN Comments c ON p.post_id = c.post_id
GROUP BY p.post_id, u.username, u.email
ORDER BY total_likes DESC, total_comments DESC;

-- A simple query to view the top 10 most engaging posts
SELECT *
FROM PostEngagement
LIMIT 10;

-- 2. Use Window Functions for Rankings
-- This is a key deliverable for advanced SQL. We'll rank posts by engagement score.
-- The engagement score is defined as (total likes * 0.7) + (total comments * 0.3).
-- We'll use a Common Table Expression (CTE) to calculate the score first.
WITH PostScores AS (
    SELECT
        p.post_id,
        p.post_content,
        u.username,
        (COUNT(DISTINCT l.like_id) * 0.7 + COUNT(DISTINCT c.comment_id) * 0.3) AS engagement_score
    FROM Posts p
    JOIN Users u ON p.user_id = u.user_id
    LEFT JOIN Likes l ON p.post_id = l.post_id
    LEFT JOIN Comments c ON p.post_id = c.post_id
    GROUP BY p.post_id, u.username
)
-- Now, use a window function to rank the posts based on their engagement score.
SELECT
    post_id,
    post_content,
    username,
    engagement_score,
    RANK() OVER (ORDER BY engagement_score DESC) AS post_rank
FROM PostScores
ORDER BY post_rank;

-- 3. Write a Trigger to Update Like Counts
-- This trigger will automatically update a counter column on the Posts table
-- whenever a new like is added, which is a common performance optimization.

-- First, alter the Posts table to add a likes_count column.
ALTER TABLE Posts
ADD COLUMN likes_count INT DEFAULT 0;

-- Then, create the trigger function.
CREATE OR REPLACE FUNCTION update_likes_count()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE Posts
    SET likes_count = likes_count + 1
    WHERE post_id = NEW.post_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Finally, create the trigger that calls the function.
CREATE TRIGGER trg_update_likes_count
AFTER INSERT ON Likes
FOR EACH ROW
EXECUTE FUNCTION update_likes_count();

-- To see it in action, you would insert a new like:
-- INSERT INTO Likes (post_id, user_id, like_date) VALUES (101, 1, NOW());
-- SELECT post_content, likes_count FROM Posts WHERE post_id = 101;
-- The `likes_count` should now be incremented automatically.

-- 4. Export Engagement Reports
-- This query provides a comprehensive report of a user's engagement.
-- We can join all the tables to get a full picture.
SELECT
    u.username,
    p.post_id,
    p.post_content,
    p.post_date,
    COUNT(DISTINCT l.like_id) AS total_likes,
    COUNT(DISTINCT c.comment_id) AS total_comments
FROM Users u
LEFT JOIN Posts p ON u.user_id = p.user_id
LEFT JOIN Likes l ON p.post_id = l.post_id
LEFT JOIN Comments c ON p.post_id = c.post_id
GROUP BY u.username, p.post_id, p.post_content, p.post_date
ORDER BY u.username, p.post_date DESC;