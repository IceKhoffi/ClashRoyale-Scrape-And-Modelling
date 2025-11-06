import pandas as pd
from tqdm import tqdm
from google_play_scraper import Sort, reviews
import time


def scrape_ps_reviews(
        app_id: str,
        total_reviews: int,
        lang: str = 'en',
        country: str = 'us',
        batch_size: int = 200,
        delay_seconds: int = 10
) -> pd.DataFrame:
    """
    scraper_ps_reviews: Scrapes Google Play Store Reviews by app ID

    Returns:
        pd.DataFrame: Collected reviews
    """

    all_reviews = []
    token = None

    print("Scraping Started!")
    
    while len(all_reviews) < total_reviews:
        try:
            result, token = reviews(
                app_id,
                lang = lang,
                country = country,
                sort = Sort.MOST_RELEVANT,
                count = batch_size,
                continuation_token = token
            )
            all_reviews.extend(result)

            curr_collected = len(all_reviews)
            tqdm.write(f"{curr_collected} has been collected!")

            # No reviews returned
            if not token and len(result) < batch_size:
                print("No more reviews found")
                break

            # Delay
            if token and curr_collected < total_reviews:
                time.sleep(delay_seconds)
        except:
            print(f"Error: {e}")
            break
    
    if not all_reviews:
        print("Error no reviews collected")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_reviews)[['userName', 'score', 'at', 'content']]
    df.columns = ['user_name', 'rating', 'date', 'review']

    # make sure no missing values or no content
    df = df.dropna(subset=['review'])
    df = df[df['review'].str.strip() != ''].reset_index(drop=True)

    df = df.head(total_reviews)
    return df

if __name__ == '__main__':
    clash_royal_data = scrape_ps_reviews(
        app_id = 'com.supercell.clashroyale',
        total_reviews = 1000,
        lang = 'en',
        country = 'us',
        delay_seconds = 10
    )

    if clash_royal_data is not None and not clash_royal_data.empty:
        clash_royal_data.to_csv('clash_royal_data.csv', index=False)
    else:
        print("Error on Output!")