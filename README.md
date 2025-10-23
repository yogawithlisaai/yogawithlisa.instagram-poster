# Instagram Auto Poster (Own the Code)

This kit posts one image per day to Instagram using the Instagram Graph API.
- Edit captions.csv with your posts (image_url, caption, date, posted).
- Add IG_USER_ID and IG_LONG_LIVED_TOKEN as GitHub Actions secrets.
- The workflow runs daily and marks posted rows as TRUE.

Image URLs must be public (e.g., raw GitHub URL, S3, or your website).
