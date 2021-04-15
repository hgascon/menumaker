import argparse
import ast
import os
import shutil
import sys
import yaml
import inquirer
from inquirer.themes import GreenPassion
from yaml import CDumper
from yaml.representer import SafeRepresenter
import pandas as pd
from ics import Calendar, Event
import datetime
import ciso8601
from tabulate import tabulate
from collections import defaultdict


class TSDumper(CDumper):
    pass


def timestamp_representer(dumper, data):
    return SafeRepresenter.represent_datetime(dumper, data.to_pydatetime())


TSDumper.add_representer(datetime.datetime, SafeRepresenter.represent_datetime)
TSDumper.add_representer(pd.Timestamp, timestamp_representer)


class Menumaker(object):

    def __init__(self, start_date, days):
        self.config = yaml.load(open("./config.yaml").read(), Loader=yaml.FullLoader)
        self.groups = yaml.load(open("./groups.yaml").read(), Loader=yaml.FullLoader)
        self.ingredients = {i: g for g, l in self.groups.items() for i in l}
        self.recipes = self._load_recipes()
        self.menu = pd.DataFrame(columns=["date", "day", "meal", "recipe", "ingredients"])
        self.update_iteration = 0
        self.start_date = start_date
        self.days = days

    def _load_recipes(self):
        recipes = pd.json_normalize(yaml.load(open("./recipes.yaml").read(),
                                              Loader=yaml.FullLoader))
        recipes['date'] = pd.to_datetime(recipes['date'])
        recipes['count'] = recipes['count'].astype(int)

        # create empty columns for different groups
        for g in self.groups.keys():
            recipes[g] = recipes.shape[0] * [False]

        def set_groups(row):
            for i in row['ingredients']:
                try:
                    row[self.ingredients[i]] = True
                except KeyError:
                    pass
            return row

        recipes = recipes.apply(set_groups, axis=1)
        recipes['new_date'] = recipes['date']
        return recipes

    def build_menu(self):

        # find dates of menu days from the starting date
        if self.start_date is not None:
            start_date = ciso8601.parse_datetime(self.start_date)
        else:
            today = datetime.date.today()
            # if start_date is not defined we set it to next Monday
            next_monday_date = today + datetime.timedelta(days=-today.weekday(), weeks=1)
            start_date = next_monday_date
            self.start_date = start_date
        start_weekday = start_date.weekday()

        weekday_meals_d = self.config[0]['weekdays']
        weekdays = list(weekday_meals_d.keys())
        date_weekday_d = {}
        menu_date = start_date
        menu_weekday = start_weekday
        for i in range(self.days):
            date_weekday_d[menu_date] = weekdays[menu_weekday]
            menu_date += datetime.timedelta(days=1)
            menu_weekday = menu_date.weekday()

        # find index of recipe in day, meal and with groups
        for menu_date, menu_weekday in date_weekday_d.items():
            for meal, groups in weekday_meals_d[menu_weekday].items():
                # select the recipe with the oldest last date
                idx = self._select_recipe_index(meal, groups)
                # create entry for the menu DataFrame
                meal_time = datetime.time(*[int(i)for i in self.config[0][meal].split(':')])
                t = datetime.datetime.combine(menu_date, meal_time)
                d = {"day": menu_weekday,
                     "date": t.strftime("%Y-%m-%d %H:%M:%S"),
                     "meal": meal,
                     "recipe": self.recipes.at[idx, "recipe"],
                     "ingredients": self.recipes.at[idx, "ingredients"],
                     "notes": self.recipes.at[idx, 'notes'],
                     "recipe_id": int(idx)}

                # update provisional new date
                self.recipes.at[idx, 'new_date'] = t
                # add recipe to menu
                self.menu = self.menu.append(d, ignore_index=True)

        # previous_update_idx = None
        update_idx = self._verify_menu()
        while update_idx != "break":
            question = [
                inquirer.List('group',
                              message=f"Select new recipe",
                              choices=list(self.recipes.sort_values(by=['new_date'])['recipe'].values)
                              ),
            ]
            print()
            answer = inquirer.prompt(question, theme=GreenPassion())['group']
            idx = self.recipes[self.recipes['recipe'] == answer].index[0]
            old_recipe_idx = self.menu.at[update_idx, 'recipe_id']
            # and update the menu with the selected recipe
            self.menu.at[update_idx, 'recipe'] = answer
            self.menu.at[update_idx, 'recipe_id'] = idx
            self.menu.at[update_idx, 'ingredients'] = self.recipes.at[idx, 'ingredients']
            # we assign the menu date to the selected recipe
            self.recipes.at[idx, 'new_date'] = self.recipes.at[old_recipe_idx, 'new_date']
            # and restore the previous date of the discarded recipe
            self.recipes.at[old_recipe_idx, 'new_date'] = self.recipes.at[old_recipe_idx, 'date']
            update_idx = self._verify_menu()

        self._update_recipes()
        self._save_recipes()

    def _update_recipes(self):
        self.recipes.loc[self.menu['recipe_id'], 'count'] += 1
        self.recipes['date'] = self.recipes['new_date']

    def _save_recipes(self):
        recipes = self.recipes.drop(columns=['new_date'] + list(self.groups.keys()), axis=1)
        shutil.copyfile('./recipes.yaml', './recipes.yaml.bak')
        with open('./recipes.yaml', 'w') as f:
            yaml.dump(
                recipes.set_index('recipe').reset_index().to_dict(orient='records'),
                f, sort_keys=False, width=100, indent=4, allow_unicode=True,
                Dumper=TSDumper,
            )
        print(f"[INFO] Recipes file updated!")

    def _save_groups(self):
        updated_groups = defaultdict(list)
        for ingredient, group in self.ingredients.items():
            updated_groups[group].append(ingredient)

        # backup groups file and save update version
        shutil.copyfile('./groups.yaml', './groups.yaml.bak')
        with open('./groups.yaml', 'w') as f:
            yaml.dump(dict(updated_groups), f, indent=4, allow_unicode=True)
        print(f"[INFO] Food group file updated!")

    def _save_menu(self):
        # save menu in log file
        self.menu[['date', 'recipe']].to_csv('./menu.log',
                                             mode='a',
                                             header=False,
                                             index=False)

    def _select_recipe_index(self, meal, groups, sort_by="new_date"):
        try:
            groups_idx = self.recipes[groups.replace(' ', '').split(',')]
            selected = self.recipes[self.recipes[meal] & groups_idx.all(axis=1)].sort_values(sort_by)
            if self.update_iteration >= selected.shape[0]:
                self.update_iteration = 0
            idx = selected.index[self.update_iteration]
        except KeyError:
            # if a specific recipe is give for a day, no groups are
            # matched and the specific recipe is selected
            idx = self.recipes[self.recipes["recipe"] == groups].index[0]
        return idx

    def _verify_menu(self):
        show = ['day', 'meal', 'recipe', 'date']
        os.system('clear')
        print(tabulate(self.menu.drop(columns=['ingredients',
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

    def export_menu_calendar(self):
        c = Calendar()
        shopping_list = []
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
            try:
                e.description = '\n'.join(row['ingredients']) + f"\n\n{row['notes']}"
                shopping_list.append('\n'.join(row['ingredients']))
            except TypeError:
                e.description = "Please fill the ingredientes for this recipe!"
                shopping_list.append(f"Ingredients for {row['recipe']}")
            c.events.add(e)

        e = Event()
        e.name = "Shopping List"
        e.begin = self.start_date
        e.description = '\n'.join(shopping_list)
        e.make_all_day()
        c.events.add(e)
        fname = "menus/menu_{}.ics".format(self.start_date)
        with open(fname, 'w') as my_file:
            my_file.writelines(c)
        os.system(f"open {fname}")

    def consolidate_ingredients(self):
        all_ingredients = set([i for l in self.recipes['ingredients'].values
                               for i in l])
        nogroup_ingredients = [ing for ing in all_ingredients
                               if ing not in self.ingredients and ing]
        nogroup_ingredients.sort()
        i = 0
        for ingredient in nogroup_ingredients:
            while True:
                question = [
                    inquirer.List('group',
                                  message=f"Select food group for \"{ingredient}\"",
                                  choices=['REWRITE INGREDIENT'] + list(self.groups.keys()),
                                  ),
                ]
                os.system('clear')
                print(f"[{i}/{len(nogroup_ingredients)}]\n")
                answer = inquirer.prompt(question, theme=GreenPassion())['group']
                if answer == 'REWRITE INGREDIENT':
                    ingredient_rewritten = input("[>] Write new name for ingredient: ")
                    self.recipes['ingredients'] = self.recipes['ingredients'].astype(str) \
                        .str.replace(ingredient, ingredient_rewritten).apply(ast.literal_eval)
                    ingredient = ingredient_rewritten
                    self._save_recipes()
                else:
                    self.ingredients[ingredient] = answer
                    i += 1
                    self._save_groups()
                    break

    def reset(self):
        self.recipes.at[:, 'date'] = pd.to_datetime(0)
        self.recipes.at[:, 'count'] = 0
        self._save_recipes()
        print(f"[INFO] Dates in recipes file reset!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--date', '-D', default=None,
                        help="First day of menu <YYYY-MM-DD> (default: Next Monday)")
    parser.add_argument('--days', '-d', default=7, type=int,
                        help="Number of days in menu (default: 7)")
    parser.add_argument('--groups', '-g', dest='g', default=False, action='store_true',
                        help="Consolidate new ingredients in groups and rewrite wrong spellings.")
    parser.add_argument('--reset', '-r', dest='r', default=False, action='store_true',
                        help="Reset last date of all recipes.")
    args = parser.parse_args()
    mm = Menumaker(args.date, args.days)
    if args.g:
        mm.consolidate_ingredients()
    elif args.r:
        mm.reset()
    else:
        mm.build_menu()
        mm.export_menu_calendar()
    sys.exit(1)
