
import os
import sys
import time
import pandas as pd

from typing import Union
from rich import console
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import element_to_be_clickable as clickable
from Crypto.Random import random


HACKERNEWS_ROOT = 'https://news.ycombinator.com'
HACKERNEWS_LOGIN = f'{HACKERNEWS_ROOT}/login'
CRAWL_DELAY = 30


class Log:
    console = console.Console()
    console.clear()

    @staticmethod
    def debug(*objects):
        Log.console.print(*objects)

    @staticmethod
    def write(*text):
        Log.console.log(*text)

    @staticmethod
    def error():
        Log.console.print_exception()
        sys.exit(0)


def xpath(query: str) -> tuple[str, str]:
    return ('xpath', query)


def randomize_crawl_delay(rand_range: int, delay=CRAWL_DELAY):
    rand_range = min(rand_range, CRAWL_DELAY)
    rand_range = int(rand_range / 2)
    return random.randint(delay - rand_range, delay + rand_range)


def start_selenium_driver():
    Log.write('starting Selenium driver')
    options = FirefoxOptions()
    options.headless = True
    driver = Firefox(options=options)
    return driver


def login(driver, wait, user, password):
    Log.write(f'logging in as {user}')
    driver.get(HACKERNEWS_LOGIN)

    # the only discernable difference betweem the login form
    # and the create account form is that the login form is
    # autofocused
    login_form = xpath('//form[table/tbody/tr/td/input[@autofocus="true"]]')
    wait.until(clickable(login_form))

    login_form = driver.find_element(*login_form)

    username_input = login_form.find_element_by_xpath('//input[@type="text"]')
    password_input = login_form.find_element_by_xpath('//input[@type="password"]')
    login_button = login_form.find_element_by_xpath('//input[@type="submit"]')

    username_input.send_keys(user)
    password_input.send_keys(password)
    login_button.click()


def validate_user(driver: Firefox, wait: WebDriverWait, user: str):
    Log.write('validating login')
    user_anchor = xpath('//a[@id="me"]')
    wait.until(clickable(user_anchor))
    user_anchor = driver.find_element(*user_anchor)
    assert user == user_anchor.text


def new_hackernews_session(wait_timeout=120):
    user, password = os.getenv('HN_USER'), os.getenv('HN_PASSWORD')
    driver = start_selenium_driver()
    wait = WebDriverWait(driver, wait_timeout)

    login(driver, wait, user, password)
    validate_user(driver, wait, user)
    return driver, wait


def extract_posts(driver: Firefox, wait: WebDriverWait):
    posts = xpath('//tr[@class="athing"]')
    subtexts = xpath('//td[@class="subtext"]')

    wait.until(clickable(posts))
    posts = driver.find_elements(*posts)
    subtexts = driver.find_elements(*subtexts)

    posts_w_subtext = list(zip(posts, subtexts))

    records = list()
    for post, subtext in posts_w_subtext:
        post_id = int(post.get_attribute('id'))

        rank = post.find_element_by_xpath('.//span[@class="rank"]')
        rank = int(rank.text[:-1])

        title = post.find_element_by_xpath('.//a[@class="titlelink"]')
        link = title.get_attribute('href')
        title = title.text

        timestamp = subtext.find_element_by_xpath('.//span[@class="age"]')
        timestamp = timestamp.get_attribute('title')

        try:
            user = subtext.find_element_by_xpath('.//a[@class="hnuser"]')
            user = user.text

            score = subtext.find_element_by_xpath('.//span[@class="score"]')
            score = score.text
            score = int(score.split(' ')[0])

        except NoSuchElementException:
            user = None
            score = None

        comments = f'https://news.ycombinator.com/item?id={post_id}'
        user_profile = f'https://news.ycombinator.com/user?id={user}'

        records.append(dict(id=post_id, rank=rank, title=title, link=link,
                            user=user, score=score, timestamp=timestamp,
                            comments=comments, user_profile=user_profile))
    return records


def go_to_next_page(driver: Firefox, wait: WebDriverWait):
    more_anchor = xpath('//a[@class="morelink"]')
    wait.until(clickable(more_anchor))
    more_anchor = driver.find_element(*more_anchor)
    more_anchor.click()


def extract_data_from_hackernews(pages=5, polite=True, crawl_range=(10, 10)):
    # "polite" will cause the bot to adhere strictly to
    # HN's crawl delay of 30 seconds.
    # Turn this setting off to speed up extraction.
    #
    # When polite=False, crawl delays will be randomized.
    # The rate will be a random integer
    # between crawl_delay[0] + (crawl_delay[1] / 2)
    # and crawl_delay[0] - (crawl_delay[1] / 2)
    # eg (10, 10) -> randint(5, 15)
    #
    # Note: requestig too quickly will result in an IP ban.
    # To unban an IP: https://news.ycombinator.com/unban?ip=<ip address>

    try:
        existing_posts = pd.read_parquet('hackernews_posts.snappy.parquet')
    except FileNotFoundError:
        existing_posts = pd.DataFrame([], columns=('id', 'rank', 'title', 'link',
                                                   'user', 'score', 'timestamp',
                                                   'comments', 'user_profile'))
        existing_posts = existing_posts.set_index('id')

    if polite:
        Log.write(f'polite scraping enabled. crawl delay set to {CRAWL_DELAY} seconds')
    else:
        Log.write('[yellow]!!WARNING!! polite scraping is disabled')
        Log.write('[yellow]crawling too quickly will result in an IP ban')

    driver, wait = new_hackernews_session()

    all_posts = list()
    page = 1
    while True:
        Log.write(f'scraping {driver.current_url}')
        current_page_posts = extract_posts(driver, wait)
        all_posts += current_page_posts

        Log.write(f'{len(current_page_posts)} posts scraped ({len(all_posts)} total)')

        if page < pages:
            crawl_delay = randomize_crawl_delay(*crawl_range) if not polite else CRAWL_DELAY
            Log.write(f'sleeping {crawl_delay} seconds')

            time.sleep(crawl_delay)
            go_to_next_page(driver, wait)
            page += 1
            continue

        break

    driver.quit()

    posts = pd.DataFrame(all_posts)
    posts = pd.concat([existing_posts, posts]).drop_duplicates()
    posts.to_parquet('hackernews_posts.snappy.parquet')


def extract_users_from_posts(polite=True, crawl_range=(10, 10)):
    posts = pd.read_parquet('hackernews_posts.snappy.parquet')
    posts = posts.set_index('id')

    posters = posts['user_profile'].unique()
    comments = posts['comments'].to_list()

    all_users = set(posters)

    driver, _ = new_hackernews_session()

    n = 0
    while True:
        comment_section = comments[n]

        Log.write(f'scraping {comment_section}')
        driver.get(comment_section)

        users = driver.find_elements_by_xpath('//a[@class="hnuser"]')
        users = set(f'https://news.ycombinator.com/user?id={u.text}' for u in users)
        all_users = all_users.union(users)

        Log.write(f'{len(users)} users scraped ({len(all_users)} total)')

        n += 1
        if n < len(comments):
            crawl_delay = randomize_crawl_delay(*crawl_range) if not polite else CRAWL_DELAY
            Log.write(f'sleeping {crawl_delay} seconds')
            time.sleep(crawl_delay)
            continue
        break

    users = pd.DataFrame(all_users, columns=('users',))
    users.to_parquet('hackernews_user_profiles.snappy.parquet')

    driver.quit()


def extract_user_profiles(sample_size: int = None, polite=True, crawl_range=(10, 10)):
    users = pd.read_parquet('hackernews_user_profiles.snappy.parquet')
    existing_profiles = pd.read_parquet('hackernews_users.snappy.parquet')

    if sample_size is None:
        sample_size = len(users)

    Log.write(f'{len(existing_profiles)} profiles loaded')

    users = users[~users['users'].isin(existing_profiles['profile'])]
    sample = users['users'].sample(sample_size).to_list()

    driver, _ = new_hackernews_session()

    all_users = list()
    n = 0
    while True:
        user_profile = sample[n]
        Log.write(f'scraping {user_profile}')
        driver.get(user_profile)
        about_section = '//tr[td[text()="about:"]]/td[@style="overflow:hidden;"]'
        about_section = driver.find_element_by_xpath(about_section)
        record = (user_profile.split('=')[-1], about_section.text, user_profile)
        all_users.append(record)

        n += 1
        if n < len(sample):
            crawl_delay = randomize_crawl_delay(*crawl_range) if not polite else CRAWL_DELAY
            Log.write(f'sleeping {crawl_delay} seconds')
            time.sleep(crawl_delay)
            continue
        break

    profiles = pd.DataFrame(all_users, columns=('user', 'about', 'profile'))
    profiles = pd.concat([existing_profiles, profiles])
    profiles = profiles.drop_duplicates()
    Log.write(f'{len(profiles)} unique profiles downloaded')
    profiles.to_parquet('hackernews_users.snappy.parquet')

    driver.quit()


def get_articles_by_keyword(keywords: Union[str, list[str]]):
    posts = pd.read_parquet('hackernews_posts.snappy.parquet')
    lower_title = posts['title'].str.lower()

    if isinstance(keywords, str):
        keywords = [keywords]

    all_posts = list()

    for keyword in keywords:
        keyword_posts = posts[lower_title.str.contains(keyword)]
        all_posts.append(keyword_posts)

    return pd.concat(all_posts).drop_duplicates()


if __name__ == '__main__':
    extract_data_from_hackernews(1, polite=False)
    relevant_hn_posts = [get_articles_by_keyword('russia'),
                         get_articles_by_keyword(['ukraine', 'ukranian']),
                         get_articles_by_keyword('belarus'),
                         get_articles_by_keyword(['china', 'chinese']),
                         get_articles_by_keyword('taiwan'),
                         get_articles_by_keyword('japan')]

    relevant_hn_posts = pd.concat(relevant_hn_posts)
    relevant_hn_posts = relevant_hn_posts[['title', 'link', 'comments', 'user', 'score']]

    grouping = ['title', 'link', 'comments', 'user']
    relevant_hn_posts = relevant_hn_posts.groupby(grouping).max('score')
    relevant_hn_posts = relevant_hn_posts.reset_index()
    relevant_hn_posts = relevant_hn_posts.sort_values(by='score', ascending=False)

    Log.debug(relevant_hn_posts)