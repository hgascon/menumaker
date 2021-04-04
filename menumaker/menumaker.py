import argparse
import os
import shutil
import sys
import yaml
from yaml import CDumper
from yaml.representer import SafeRepresenter
import pandas as pd
from ics import Calendar, Event
import datetime
import ciso8601
from tabulate import tabulate


class TSDumper(CDumper):
    pass


def timestamp_representer(dumper, data):
    return SafeRepresenter.represent_datetime(dumper, data.to_pydatetime())


TSDumper.add_representer(datetime.datetime, SafeRepresenter.represent_datetime)
TSDumper.add_representer(pd.Timestamp, timestamp_representer)


class Menumaker(object):

    def __init__(self, start_date):
        self.config = yaml.load(open("./config.yaml").read(), Loader=yaml.FullLoader)
        self.groups = yaml.load(open("./groups.yaml").read(), Loader=yaml.FullLoader)[0]
        self.recipes = self.load_recipes()
        self.menu = pd.DataFrame(columns=["date", "day", "meal", "recipe", "ingredients"])
        self.update_iteration = 0
        self.start_date = start_date

    def load_recipes(self):
        recipes = pd.json_normalize(yaml.load(open("./recipes.yaml").read(),
                                                   Loader=yaml.FullLoader))
        recipes['date'] = pd.to_datetime(recipes['date'])
        recipes['count'] = recipes['count'].astype(int)

        # build mapping ingredient -> group
        ingredients = {i: g for g, l in self.groups.items() for i in l}

        # create empty columns for different groups
        for g in self.groups.keys():
            recipes[g] = recipes.shape[0] * [False]

        def set_groups(row):
            for i in row['ingredients']:
                try:
                    row[ingredients[i]] = True
                except KeyError:
                    pass
            return row

        recipes = recipes.apply(set_groups, axis=1)
        recipes['new_date'] = recipes['date']
        return recipes

    def build_menu(self):

        # find dates of menu days from the starting date
        day_meals = self.config[0]['weekdays']
        dates = {}
        if self.start_date is not None:
            current_date = ciso8601.parse_datetime(self.start_date)
        else:
            today = datetime.date.today()
            next_monday = today + datetime.timedelta(days=-today.weekday(), weeks=1)
            self.start_date = next_monday
            current_date = next_monday
        for day in day_meals.keys():
            dates[day] = current_date
            current_date += datetime.timedelta(days=1)

        # find index of recipe in day, meal and with groups
        for day, meals in day_meals.items():
            for meal, groups in meals.items():
                # select the recipe with the oldest last date
                idx = self.select_recipe_index(meal, groups) 
                # create entry for the menu DataFrame
                meal_time = datetime.time(*[int(i)for i in self.config[0][meal].split(':')])
                t = datetime.datetime.combine(dates[day], meal_time)
                d = {"day": day,
                     "date": t.strftime("%Y-%m-%d %H:%M:%S"),
                     "meal": meal,
                     "groups": groups,
                     "recipe": self.recipes.at[idx, "recipe"],
                     "ingredients": self.recipes.at[idx, "ingredients"],
                     "count": self.recipes.at[idx, 'count'],
                     "notes": self.recipes.at[idx, 'notes'],
                     "recipe_id": idx}

                # update provisional new date
                self.recipes.at[idx, 'new_date'] = t
                # add recipe to menu
                self.menu = self.menu.append(d, ignore_index=True)

        previous_update_idx = None
        update_idx = self.verify_menu()
        while update_idx != "break":
            if update_idx != previous_update_idx and previous_update_idx is not None:
                self.update_iteration = 0
            self.update_iteration += 1
            self.update_menu(update_idx)
            previous_update_idx = update_idx
            update_idx = self.verify_menu()

        # backup recipes file and save update version
        shutil.copyfile('./recipes.yaml', './recipes.yaml.bak')
        self.recipes.loc[self.menu['recipe_id'].astype(int), 'count'] += 1
        self.recipes['date'] = self.recipes['new_date']
        self.recipes.drop(columns=['new_date'] + list(self.groups.keys()), axis=1, inplace=True)
        text = yaml.dump(
            self.recipes.set_index('recipe').reset_index().to_dict(orient='records'),
            sort_keys=False, width=100, indent=4, allow_unicode=True,
            default_flow_style=None, Dumper=TSDumper,
        )
        with open('./recipes.yaml', 'w') as f:
            f.write(text)

        # save menu in log file
        self.menu[['date', 'recipe']].to_csv('./menu.log',
                                             mode='a',
                                             header=False,
                                             index=False)

    def select_recipe_index(self, meal, groups):
        try:
            selected = self.recipes[self.recipes[meal]][self.recipes[groups.replace(' ', '').split(',')].all(axis=1)].sort_values("new_date")
            if self.update_iteration >= selected.shape[0]:
                self.update_iteration = 0
            idx = selected.index[self.update_iteration]
        except KeyError:
            # if a specific recipe is give for a day, no groups are
            # matched and the specific recipe is selected
            idx = self.recipes[self.recipes["recipe"] == groups].index[0] 
        return idx 

    def verify_menu(self):
        show = ['day', 'meal', 'recipe', 'date']
        os.system('clear')
        print(tabulate(self.menu.drop(columns=['ingredients',
                                               'groups',
                                               'count',
                                               'recipe_id'])[show], headers='keys', tablefmt='psql'))
        while True:
            idx = input("Select a meal number to change it or write \"save\" to accept the menu: ")
            if idx != "save":
                try:
                    idx = int(idx)
                    if idx in self.menu.index.values:
                        return idx
                except ValueError:
                    continue
            else:
                return "break"

    def update_menu(self, update_idx):
        # restore previous date from unwanted meal
        old_recipe_id = self.menu.at[update_idx, 'recipe_id']
        self.recipes.at[old_recipe_id, 'new_date'] = self.recipes.at[old_recipe_id, 'date']
        meal = self.menu.at[update_idx, 'meal']
        groups = self.menu.at[update_idx, 'groups']
        idx = self.select_recipe_index(meal, groups)
        self.menu.at[update_idx, 'recipe_id'] = idx
        self.menu.at[update_idx, 'recipe'] = self.recipes.at[idx, 'recipe']
        self.menu.at[update_idx, 'ingredients'] = self.recipes.at[idx, 'ingredients']
        self.menu.at[update_idx, 'count'] = self.recipes.at[idx, 'count']

    def export_menu_calendar(self):
        c = Calendar()
        # create calendar event
        for _, row in self.menu.iterrows():
            e = Event()
            e.name = '[' + row['meal'].capitalize() + '] ' + row['recipe']
            t = datetime.datetime.strptime(row['date'], "%Y-%m-%d %H:%M:%S") - datetime.timedelta(hours=2)
            e.begin = t.strftime("%Y-%m-%d %H:%M:%S")
            if row['meal'] == 'lunch':
                e.duration = {"minutes": 30}
            else:
                e.duration = {"minutes": 60}
            e.description = '\n'.join(row['ingredients']) + f"\n\n{row['notes']}"
            c.events.add(e)

        e = Event()
        shopping_list = "\n".join(list(set([i for l in self.menu.ingredients.values for i in l])))
        e.name = "Shopping List"
        e.begin = self.start_date.strftime("%Y-%m-%d %H:%M:%S")
        e.description = shopping_list
        e.make_all_day()
        c.events.add(e)
        fname = "menus/menu_{}.ics".format(self.start_date.strftime("%Y%m%d"))
        with open(fname, 'w') as my_file:
            my_file.writelines(c)
        os.system(f"open {fname}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--date', '-d', default=None,
                        help="First day of menu <YYYY-MM-DD> (Default: Next Monday)")
    # TODO option to find ingredients w/o group and assign them in groups.yaml
    # TODO option to consolidate ingredients
    # TODO test different start dates
    args = parser.parse_args()
    mm = Menumaker(args.date)
    mm.build_menu()
    mm.export_menu_calendar()
    sys.exit(1)
