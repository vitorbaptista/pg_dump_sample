INSERT INTO users (id, username, email, created_at) VALUES
    (1, 'alice',   'alice@example.com',   '2024-01-01 10:00:00'),
    (2, 'bob',     'bob@example.com',     '2024-01-02 11:00:00'),
    (3, 'charlie', 'charlie@example.com', '2024-01-03 12:00:00'),
    (4, 'diana',   'diana@example.com',   '2024-01-04 13:00:00'),
    (5, 'eve',     'eve@example.com',     '2024-01-05 14:00:00');

SELECT setval('users_id_seq', 5);

INSERT INTO posts (id, user_id, title, body, created_at) VALUES
    (1, 1, 'First Post',       'Hello world!',                    '2024-02-01 10:00:00'),
    (2, 1, 'Second Post',      'Another post by Alice.',          '2024-02-02 11:00:00'),
    (3, 2, 'Bob''s Post',      'Bob here.',                       '2024-02-03 12:00:00'),
    (4, 3, 'Charlie''s Post',  'Charlie checking in.',            '2024-02-04 13:00:00'),
    (5, 4, 'Diana''s Post',    'Diana''s first post.',            '2024-02-05 14:00:00'),
    (6, 5, 'Eve''s Post',      'Eve says hello.',                 '2024-02-06 15:00:00'),
    (7, 1, 'Alice Again',      'Alice with a third post.',        '2024-02-07 16:00:00'),
    (8, 2, 'Bob Returns',      'Bob is back.',                    '2024-02-08 17:00:00');

SELECT setval('posts_id_seq', 8);

INSERT INTO comments (id, post_id, user_id, body, created_at) VALUES
    (1,  1, 2, 'Nice post, Alice!',           '2024-03-01 10:00:00'),
    (2,  1, 3, 'I agree with Bob.',            '2024-03-01 11:00:00'),
    (3,  2, 4, 'Interesting read.',            '2024-03-02 12:00:00'),
    (4,  3, 1, 'Welcome, Bob!',               '2024-03-03 13:00:00'),
    (5,  3, 5, 'Great to see you, Bob.',       '2024-03-03 14:00:00'),
    (6,  4, 2, 'Hey Charlie!',                 '2024-03-04 15:00:00'),
    (7,  5, 3, 'Nice one, Diana.',             '2024-03-05 16:00:00'),
    (8,  6, 1, 'Hi Eve!',                      '2024-03-06 17:00:00'),
    (9,  7, 2, 'Alice, great post again!',     '2024-03-07 18:00:00'),
    (10, 8, 3, 'Bob, welcome back!',           '2024-03-08 19:00:00');

SELECT setval('comments_id_seq', 10);
