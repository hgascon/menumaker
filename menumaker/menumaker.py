import argparse
import sys
import yaml
import pandas as pd
from ics import Calendar, Event
import datetime
import ciso8601
from tabulate import tabulate


class Menumaker(object):

    def __init__(self, start_date):
        self.config = yaml.load(open("./config.yaml").read(), Loader=yaml.FullLoader)
        self.recipes = self.load_recipes()
        self.menu = pd.DataFrame(columns=["date", "day", "meal", "recipe", "ingredients"])
        self.update_iteration = 0
        self.start_date = start_date

    @staticmethod
    def load_recipes():
        recipes = pd.json_normalize(yaml.load(open("./recipes.yaml").read(),
                                                   Loader=yaml.FullLoader))
        recipes['date'] = pd.to_datetime(recipes['date'])
        recipes['count'] = recipes['count'].astype(int)

        # build mapping ingredient -> group
        groups = yaml.load(open("./groups.yaml").read(), Loader=yaml.FullLoader)[0]
        ingredients = {i: g for g, l in groups.items() for i in l}

        # create empty columns for different groups
        for g in groups.keys():
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
                     "recipe_id": idx}

                # update provisional new date
                self.recipes.at[idx, 'new_date'] = t
                # add recipe to menu
                self.menu = self.menu.append(d, ignore_index=True)

        previous_update_idx = None
        update_idx = self.verify_menu()
        while update_idx:
            if update_idx != previous_update_idx and previous_update_idx is not None:
                self.update_iteration = 0
            self.update_iteration += 1
            self.update_menu(update_idx)
            previous_update_idx = update_idx
            update_idx = self.verify_menu()

    # TODO update dates and counts after confirmation of menu

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
        print(tabulate(self.menu, headers='keys', tablefmt='psql'))
        while True:
            idx = input("Select a meal number to change it or write \"save\" to accept the menu:")
            if idx != "save":
                try:
                    idx = int(idx)
                    if idx in self.menu.index.values:
                        return idx
                except ValueError:
                    continue
            else:
                return None

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




# def create_calendar_menu(self):
    #     c = Calendar()
    #     # create calendar event
    #     e = Event()
    #     e.name = "[" + meal.capitalize() + "] " + df.at[idx, "recipe"]
    #     e.begin = t.strftime("%Y-%m-%d %H:%M:%S")
    #     e.duration = {"minutes": 30}
    #     e.description = "\n".join(df.at[idx, "ingredients"])
    #     c.events.add(e)
    #     shopping_list = "\n".join(list(set([i for l in menu.ingredients.values for i in l])))
    #     e.name = "Shopping List"
    #     #TODO change next_monday here with the date of the first menu date
    #     e.begin = next_monday.strftime("%Y-%m-%d %H:%M:%S")
    #     e.description = shopping_list
    #     e.make_all_day()
    #     c.events.add(e)
    #     fname = "menu_{}.ics".format(next_monday.strftime("%Y%m%d"))
    #     with open(fname, 'w') as my_file:
    #         my_file.writelines(c)




if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--date', '-d', default=None,
                        help="First day of menu <DD-MM> (Default: Next Monday)")
    args = parser.parse_args()
    Menumaker(args.date).build_menu()
    sys.exit(1)
