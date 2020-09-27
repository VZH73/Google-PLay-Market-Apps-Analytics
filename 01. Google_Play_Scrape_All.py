class Parse:
    import requests
    from bs4 import BeautifulSoup as bs

    base_url = "https://play.google.com"

    class Params:
        Category_List = {'element':'li','class':'KZnDLd'}
        Single_Category = {'element':'div', 'class':'b8cIId ReQCgd Q9MA7b'}
        See_More_Buttons = {'element':'div', 'class':'W9yFB'}

    @staticmethod
    def Items_List(url, param):
        response = Parse.requests.get(Parse.base_url + url)

        items = []

        soup = Parse.bs(response.text, 'html.parser')

        all_elements = soup.find_all(param['element'], {'class': param['class']})

        for element in all_elements:
            try:
                a = element.select_one('a')
                item_href = a.get('href')
                items.append({'href':item_href})
            except:
                continue
        return items

    @staticmethod
    def Category_List(url):
        return Parse.Items_List(url, Parse.Params.Category_List)

    @staticmethod
    def Single_Category(url):
        items = Parse.Items_List(url, Parse.Params.Single_Category)
        return [item['href'].split('/store/apps/details?id=')[1] for item in items]

    @staticmethod
    def See_More_Buttons(url):
        return Parse.Items_List(url, Parse.Params.See_More_Buttons)

    @staticmethod
    def Collect_Top(url):
        top_apps = []
        categories = Parse.Category_List(url)
        for cat in tqdm(categories):
            see_more = Parse.See_More_Buttons(cat['href'])
            if len(see_more) > 0:
                for button in see_more:
                    top_apps.extend(
                        [app_id for app_id in Parse.Single_Category(button['href']) if app_id not in top_apps])
            else:
                    top_apps.extend(
                        [app_id for app_id in Parse.Single_Category(cat['href']) if app_id not in top_apps])

        return top_apps

    @staticmethod
    def Collect_Sim(app_id):
        similar_apps_list = []
        similar_apps_btn = Parse.See_More_Buttons('/store/apps/details?id=' + app_id)
        if len(similar_apps_btn) > 0:
            for button in similar_apps_btn:
                try:
                    similar_apps_list = Parse.Single_Category(button['href'])
                    if len(similar_apps_list) > 0:
                        break
                except:
                    continue
        return similar_apps_list

from google_play_scraper import app
from tqdm import tqdm
import keyboard
import io
import os
import json

def Save_Json(data, file_name, mode="w"):
    try:
        with io.open(file_name, mode, encoding='utf8') as out_file:
            if mode == 'w':
                json.dump(data, out_file, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
            elif mode =='a':
                out_file.seek(0, os.SEEK_END)  # seek to end of file
                out_file.seek(out_file.tell() - 1, os.SEEK_SET)
                out_file.truncate()             # remove last '}' for proper append
                out_file.write(',' + json.dumps(data)[1:])
    except:
        print("Can't save the file {}".format(file_name))

def Load_Json(data, file_name):
    try:
        if os.path.exists(file_name):
            with io.open(file_name, encoding='utf8') as data_file:
                data = json.load(data_file)
    except:
        print("Can't load the file {}".format(file_name))
    return data


Top_Apps = 'Scrapped//GPA_Top.json'
Sim_Apps = 'Scrapped//GPA_Similar.json'
Final_Apps = 'Scrapped//GPA_Final.json'
Final_Apps_Data = 'Scrapped//GPA_Final_App_Data2.json'

print('Web scrapping Google Play Store. Please be patient. Press ESC to interrupt. Process could then be resumed.')

# -----------------------------------------------------
# Step One
# Loading results of previous runs
# -----------------------------------------------------
print('Step one: Loading results of previous runs')
top_list = Load_Json([], Top_Apps)
sim_list = Load_Json([], Sim_Apps)
final_list = Load_Json([], Final_Apps)
apps_all_data = Load_Json([], Final_Apps_Data)

reload_from_top = False
if len(top_list) > 0:
    print('Number of top apps loaded from file: {}'.format(len(top_list)))
else:
    reload_from_top = True

if len(sim_list) > 0:
    print('Number of similar apps loaded from file: {}'.format(len(sim_list)))
elif len(top_list) > 0:
    print('No similar apps (L1) found. Top apps will be reloaded')
    reload_from_top = True

if len(final_list) > 0:
    print('Number of apps in final list loaded from file: {}'.format(len(final_list)))

if len(apps_all_data) > 0:
    print('Number of apps with detailed data loaded from file: {}'.format(len(apps_all_data)))
    if len(set(final_list).difference(set(apps_all_data))) == 0:
        reload_from_top = (input('Data files seems to be complete. Do you want to refresh the dataset with anything new? (Y/N)').upper()+' ')[0] == 'Y'

# -----------------------------------------------------
# Step Two
# Getting top level appsy
# -----------------------------------------------------
if reload_from_top:
    print('Step two: Collecting primary search results from different categories')
    top_list = Parse.Collect_Top('/store/apps')
    Save_Json(top_list, Top_Apps)
    print('Top apps collected: {}'.format(len(top_list)))

# -----------------------------------------------------
# Step Three
# Looking for similar apps from top list
# -----------------------------------------------------
top_list_2go = list(set(top_list).difference(set(final_list)))
if len(top_list_2go) > 0:
    print('Step three: Collecting similar apps from top list')
    if len(top_list_2go) != len(top_list):
        print('Resuming data collection from {} step of total {} steps'.format(
            len(set(top_list).intersection(set(final_list))),
            len(top_list)))

    count = 0
    for app_id in tqdm(top_list_2go):
        if keyboard.is_pressed('Esc'):
            print('Process has been interrupted at step {} of total {} steps'.
                  format(len(set(top_list).intersection(set(final_list))),
                         len(top_list)))
            break
        try:
            apps = Parse.Collect_Sim(app_id)
            sim_list.extend(set(apps).difference(set(sim_list)))
            if app_id not in final_list:
                final_list.append(app_id)

            # Save intermediate results in 100 items batch
            count += 1
            if count == 100:
                Save_Json(sim_list, Sim_Apps)
                Save_Json(final_list, Final_Apps)
                count = 0
        except:
            continue

    Save_Json(sim_list, Sim_Apps)
    Save_Json(final_list, Final_Apps)
    print('Similar apps collected: {}'.format(len(sim_list)))

# -----------------------------------------------------
# Step Four
# Looking for similar apps from sim list
# -----------------------------------------------------
sim_list_2go = list(set(sim_list).difference(set(final_list)))
if len(sim_list_2go) > 0:
    print('Step four: Collecting similar apps from sim list and updating final list')
    if len(sim_list_2go) != len(sim_list):
        print('Resuming data collection from {} step of total {} steps'.format(
            len(set(sim_list).intersection(set(final_list))),
            len(sim_list)))

    count = 0
    for app_id in tqdm(sim_list_2go):
        if keyboard.is_pressed('Esc'):
            print('Process has been interrupted at step {} of total {} steps'.format(
                len(set(sim_list).intersection(set(final_list))),
                len(sim_list)))
            break
        try:
            apps = Parse.Collect_Sim(app_id)
            final_list.extend(set(apps).difference(set(final_list)))
            if app_id not in final_list:
                final_list.append(app_id)

            # Save intermediate results in 100 items batch
            count += 1
            if count == 100:
                Save_Json(final_list, Final_Apps)
                count = 0
        except:
            continue

    Save_Json(final_list, Final_Apps)
    print('Final list of apps collected: {}'.format(len(final_list)))

# -----------------------------------------------------
# Step Five
# Getting detailed app info
# -----------------------------------------------------
final_list_2go = list(set(final_list).difference(set(apps_all_data)))
if len(final_list_2go) > 0:
    print('Step five: Scrapping apps data')
    count = 0
    for app_id in tqdm(final_list_2go):
        if keyboard.is_pressed('Esc'):
            print('Process has been interrupted at step {} of total {} steps'.format(
                len(apps_all_data),
                len(apps_all_data)+len(final_list_2go)))
            break
        try:
            apps_all_data[app_id] = app(app_id, lang='en', country='us')

            # Save intermediate results in 100 items batch. As the resulting file is very big, append last 100 records to existing file, not overwrite.
            count += 1
            if count == 100:
                Save_Json({key:apps_all_data[key] for key in list(apps_all_data.keys())[-100:]}, Final_Apps_Data, 'a')
                count = 0
        except:
            # Detailed info could not be retrieved. Remove the app
            try: top_list.remove(app_id)
            except: pass
            try: sim_list.remove(app_id)
            except: pass
            try: final_list.remove(app_id)
            except: pass
            continue

# -----------------------------------------------------
# Step Six
# Saving detailed app info to the file
# -----------------------------------------------------
print('Step six: Saving to JSON text file')
Save_Json(top_list, Top_Apps)
Save_Json(sim_list, Sim_Apps)
Save_Json(final_list, Final_Apps)
Save_Json(apps_all_data, Final_Apps_Data)
print('Done. File {} has been saved with {} records out of total {} found ids'.format(
    Final_Apps_Data,
    len(apps_all_data),
    len(final_list)))


a = list()
a = [{1:2, 3:4
      }]
