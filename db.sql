WITH
all_chords as (
  SELECT DISTINCT
    trim(artist) as artist,
    trim(title) as title,
    url,
    version,
    rating,
    votes,
    difficulty,
    tonality_name,
    views,
    1 as src_order,
    1 as has_ug_tabs,
    0 as has_wywrota_tabs
  FROM read_csv_auto($ultimate_guitar_file_path)
  UNION ALL
  SELECT DISTINCT
    trim(artist) as artist,
    trim(title) as title,
    url,
    version,
    rating,
    votes,
    difficulty,
    tonality_name,
    views,
    0 as src_order,
    0 as has_ug_tabs,
    1 as has_wywrota_tabs
  FROM read_csv($wywrota_file_path, columns := {
    'artist': 'varchar',
    'title': 'varchar',
    'url': 'varchar',
    'version': 'int64',
    'rating': 'double',
    'votes': 'int64',
    'difficulty': 'varchar',
    'tonality_name': 'varchar',
    'views': 'int64'
  })
),
spotify_liked as (
  SELECT DISTINCT
    trim(artist) as artist,
    trim(title) as title
  FROM read_csv_auto($spotify_file_path)
),
songs_aggregated as (
  SELECT
    artist,
    title,
    list(
      struct_pack(
        version := version,
        url := url,
        rating := rating,
        votes := votes,
        difficulty := difficulty,
        tonality_name := tonality_name,
        views := views
      )
      order by src_order asc, version desc
    ) as chords,
    sum(views) as total_views,
    round(sum(rating * votes) / sum(votes), 2) as avg_rating,
    coalesce(max(has_ug_tabs), 0) as has_ug_tabs,
    coalesce(max(has_wywrota_tabs), 0) as has_wywrota_tabs
  FROM all_chords
  GROUP BY artist, title
)
SELECT
  songs_aggregated.*,
  spotify_liked.artist IS NOT NULL as liked_on_spotify
FROM songs_aggregated
LEFT JOIN spotify_liked USING (artist, title)
ORDER BY artist
