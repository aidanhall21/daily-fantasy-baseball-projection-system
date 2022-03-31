from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
import numpy as np
from team_reference import reference, reference_test

PATH = '/usr/local/bin/chromedriver'
driver = webdriver.Chrome(PATH)

player_names = []
player_roles = []
player_ids = []
player_team = []
player_league = []
player_park = []

for obj in reference:
    print(obj)
    driver.get('https://www.fangraphs.com/roster-resource/depth-charts/' + obj['team'])
    time.sleep(10)
    all_rows = driver.find_elements_by_tag_name('tr')
    print(len(all_rows))
    for row in all_rows:
        player_team.append(obj['abbr'])
        player_league.append(obj['lg'])
        player_park.append(obj['park_id'])
        try:
            player = row.find_element_by_css_selector("[data-stat='PLAYER']")
            if player.text[-4:] == '\nNRI':
                player.text = player.text[:-4]
            player_names.append(player.text)
            try:
                link = player.find_element_by_tag_name('a')
                player_ids.append(link.get_attribute('href').split('/')[5])
            except:
                player_ids.append('')
        except:
            player_ids.append('')
            player_names.append('')
        try:
            role = row.find_element_by_css_selector("[data-stat='STATUS']")
            player_roles.append(role.text)
        except:
            player_roles.append('B')
    d = {'id': player_ids, 'name': player_names, 'role': player_roles, 'team': player_team, 'league': player_league, 'park_id': player_park}
    df = pd.DataFrame(d, columns=['id', 'name', 'role', 'team', 'league', 'park_id'])
    df['id'].replace('', np.nan, inplace=True)
    df['name'].replace('', np.nan, inplace=True)
    df['role'].replace('', np.nan, inplace=True)
    df['team'].replace('', np.nan, inplace=True)
    df['league'].replace('', np.nan, inplace=True)
    df['park_id'].replace('', np.nan, inplace=True)
    df.dropna(inplace=True)
    df.to_csv('test_selenium_df10.csv', index=False)


#print(player_names)
#print(player_ids)
#print(player_roles)
#print(len(player_names))
#print(len(player_ids))
#print(len(player_roles))

d = {'id': player_ids, 'name': player_names, 'role': player_roles, 'team': player_team, 'league': player_league, 'park_id': player_park}
df = pd.DataFrame(d, columns=['id', 'name', 'role', 'team', 'league', 'park_id'])
df['id'].replace('', np.nan, inplace=True)
df['name'].replace('', np.nan, inplace=True)
df['role'].replace('', np.nan, inplace=True)
df['team'].replace('', np.nan, inplace=True)
df['league'].replace('', np.nan, inplace=True)
df['park_id'].replace('', np.nan, inplace=True)
df.dropna(inplace=True)
df.to_csv('test_selenium_df_final.csv', index=False)

driver.quit()