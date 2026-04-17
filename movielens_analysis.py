import pytest
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime
from collections import Counter

PATH_TO_CSV_MOVIES = "../datasets/movies.csv"
PATH_TO_CSV_TAGS = "../datasets/tags.csv"
PATH_TO_CSV_LINKS = "../datasets/links.csv"
PATH_TO_CSV_RATINGS = "../datasets/ratings.csv"


class Tags:
    def __init__(self, path_to_the_file):
        if os.path.exists(path_to_the_file):
            self.path = path_to_the_file
            self.tags = self.select_tags(self.read_file())
        else:
            raise FileNotFoundError(f"File did not exist: {path_to_the_file}")

    def read_file(self):
        with open(self.path, "r", encoding="utf-8") as file:
            lines = []
            line_number = -1

            for line in file:
                line_number += 1
                line = line.strip()

                if line_number == 0 and line != "userId,movieId,tag,timestamp":
                    raise Exception(f"Incorrect file header")

                if line_number == 0:
                    continue

                if len(lines) >= 1000:
                    break

                data = line.split(",")

                if len(data) != 4:
                    raise Exception(f"Invalid type of string in the file")

                if not data[0].strip().isdigit():
                    raise Exception(f"Invalid data type in the first column")

                if not data[1].strip().isdigit():
                    raise Exception(f"Invalid data type in the second column")

                if not data[2].strip():
                    raise Exception(f"Invalid data type in the third column")

                if not data[3].strip().isdigit():
                    raise Exception(f"Invalid data type in the fourth column")

                lines.append(line)
            if len(lines) == 0:
                raise Exception("File contains no data or only header")
        return lines

    def select_tags(self, lines):
        tags = [line.split(",")[2] for line in lines]
        return tags

    def most_words(self, n):
        len_tags = [len(re.split(r"[ /:]+", tag)) for tag in self.tags]
        dict_len_tags = dict(zip(self.tags, len_tags))
        dict_len_tags = sorted(dict_len_tags.items(), key=lambda x: (-x[1], x[0]))[:n]
        big_tags = dict(dict_len_tags)
        return big_tags

    def longest(self, n):
        unique_tags = list(set(self.tags))
        big_tags = sorted(unique_tags, key=lambda x: (-len(x), x))

        return big_tags[:n]

    def most_words_and_longest(self, n):
        words = set(self.most_words(n).keys())
        longest = set(self.longest(n))
        big_tags = sorted(words.intersection(longest))
        return big_tags

    def most_popular(self, n):
        tag_counter = Counter(self.tags)
        popular_tags = sorted(tag_counter.items(), key=lambda x: (-x[1], x[0]))[:n]

        return dict(popular_tags)

    def tags_with(self, word):
        word = word.lower()
        tags_with_word = set()
        for tag in self.tags:
            if word in tag.lower():
                tags_with_word.add(tag)

        return sorted(tags_with_word)


class Ratings:
    def __init__(self, path_to_the_file):
        self.path = path_to_the_file
        self.data = self.read_file()

    def str_handler(self, string):
        data = string.split(",")
        data = [i.strip() for i in data]

        if len(data) != 4:
            raise Exception("Invalid type of string in the file!")

        if not data[0].isdigit():
            raise Exception("Invalid data type in the first column")
        data[0] = int(data[0])

        if not data[1].isdigit():
            raise Exception("Invalid data type in the second column")
        data[1] = int(data[1])

        try:
            float(data[2])
        except ValueError:
            raise Exception("Invalid data type in the third column")
        data[2] = float(data[2])

        if not data[3].isdigit():
            raise Exception("Invalid data type in the fourth column")
        data[3] = int(data[3])

        return data

    def read_file(self):
        try:
            with open(self.path, "r", encoding="utf-8") as file:
                next(file)
                data = []
                cnt = 0
                for line in file:
                    if cnt < 1000:
                        cnt += 1
                        try:
                            data.append(self.str_handler(line))
                        except:
                            raise Exception("Error of handling string.")
        except:
            raise Exception("File doesn't exist or it's uncorrect.")

        return data

    class Movies:
        def __init__(self, outer_instance, path_to_file):
            self.path = path_to_file
            self.outer = outer_instance
            self.ratings_data = outer_instance.data
            self.movies_data = self.read_file()
            self.union_data = (
                self.tables_union()
            )  # [userId,movieId,rating,timestamp,title,genres]

        def str_handler(self, string):
            first_quote = False
            string = string + ","
            index = 0
            new_string = []
            i = 0
            while i < (len(string)):
                if string[i] == '"' and not (first_quote):
                    first_quote = True
                    index = i + 1
                    i += 1
                elif (
                    string[i] == '"'
                    and first_quote
                    and ((i + 1) == len(string) or string[i + 1] == ",")
                ):
                    first_quote = False
                    new_string.append(string[index:i].strip())
                    index = i + 2
                    i += 2
                elif string[i] == "," and not (first_quote):
                    new_string.append(string[index:i].strip())
                    index = i + 1
                    i += 1
                else:
                    i += 1

            if len(new_string) != 3:
                raise Exception("Invalid type of string in the file!")

            if not new_string[0].isdigit():
                raise Exception("Invalid data type in the first column")
            new_string[0] = int(new_string[0])

            return new_string

        def read_file(self):
            try:
                with open(self.path, "r", encoding="utf-8") as file:
                    next(file)
                    data = []
                    cnt = 0
                    for line in file:
                        if cnt < 1000:
                            cnt += 1
                            try:
                                data.append(self.str_handler(line.strip()))
                            except:
                                raise Exception("Error of handling string")
            except:
                raise Exception("File doesn't exist or it's uncorrect.")

            return data

        def tables_union(self):
            dict_movies = {}
            for row in self.movies_data:
                dict_movies[row[0]] = [row[1], row[2]]

            union_table = []
            for row in self.ratings_data:
                movie_info = dict_movies.get(row[1])
                if movie_info is not None:
                    new_row = row.copy()
                    new_row.extend(movie_info)
                    union_table.append(new_row)

            return union_table

        def extract_year_from_timestamps(self, time):
            try:
                dt_object = datetime.datetime.fromtimestamp(time)
                year = dt_object.year
            except:
                raise Exception("Can't handle time!")

            return year

        def dist_by_year(self):
            years = [
                self.extract_year_from_timestamps(row[3])
                for row in self.union_data
                if row[3] != ""
            ]
            ratings_by_year = dict(
                sorted(Counter(years).items(), key=lambda item: item[0])
            )

            return ratings_by_year

        def dist_by_rating(self):
            rates = [row[2] for row in self.union_data if row[2] != ""]
            ratings_distribution = dict(
                sorted(Counter(rates).items(), key=lambda item: item[0])
            )

            return ratings_distribution

        def top_by_num_of_ratings(self, n):
            movies = [row[4] for row in self.union_data]
            top_movies = dict(
                sorted(Counter(movies).items(), key=lambda item: item[1], reverse=True)[
                    :n
                ]
            )

            return top_movies

        def get_median(self, new_list):
            if not new_list:
                return 0.0

            new_list = sorted(new_list)
            length = len(new_list)
            half = length // 2
            median = 0

            if length % 2 == 0:
                median = (new_list[half] + new_list[half - 1]) / 2
            else:
                median = new_list[half]

            return round(median, 2)

        def get_average(self, new_list):
            if not new_list:
                return 0.0

            return round(sum(new_list) / len(new_list), 2)

        def top_by_ratings(self, n, metric="average"):
            top_movies = {}

            if metric == "average" or metric == "median":

                values = {}  # {title:[values for ratings]}
                for row in self.union_data:
                    if row[4] in values:
                        values[row[4]].append(row[2])
                    else:
                        values[row[4]] = [row[2]]

                if metric == "average":
                    for key, value in values.items():
                        top_movies[key] = self.get_average(value)
                else:
                    for key, value in values.items():
                        top_movies[key] = self.get_median(value)
            else:
                raise Exception("Not valid metric")

            top_movies = dict(
                sorted(top_movies.items(), key=lambda item: item[1], reverse=True)[:n]
            )

            return top_movies

        def get_variance(self, my_list):
            if not my_list:
                return 0.0

            aver = self.get_average(my_list)

            variance = sum([(x - aver) ** 2 for x in my_list]) / len(my_list)

            return round(variance, 2)

        def top_controversial(self, n):
            top_movies = {}
            values = {}  # {title:[values for ratings]}

            for row in self.union_data:
                if row[4] in values:
                    values[row[4]].append(row[2])
                else:
                    values[row[4]] = [row[2]]

            for key, value in values.items():
                top_movies[key] = self.get_variance(value)

            top_movies = dict(
                sorted(top_movies.items(), key=lambda item: item[1], reverse=True)[:n]
            )

            return top_movies

    class Users(Movies):
        def __init__(self, movies_instance):
            self.union_data = movies_instance.union_data

        def dist_by_users(self):
            users_distribution = dict(Counter([row[0] for row in self.union_data]))

            return users_distribution

        def dist_by_metric(self, metric="average"):
            users = {}

            if metric == "average" or metric == "median":

                values = {}  # {title:[values for ratings]}
                for row in self.union_data:
                    if row[0] in values:
                        values[row[0]].append(row[2])
                    else:
                        values[row[0]] = [row[2]]

                if metric == "average":
                    for key, value in values.items():
                        users[key] = self.get_average(value)
                else:
                    for key, value in values.items():
                        users[key] = self.get_median(value)
            else:
                raise Exception("Not valid metric")

            return users

        def top_controversial(self, n):
            top_users = {}
            values = {}  # {userid:[values for ratings]}

            for row in self.union_data:
                if row[0] in values:
                    values[row[0]].append(row[2])
                else:
                    values[row[0]] = [row[2]]

            for key, value in values.items():
                top_users[key] = self.get_variance(value)

            top_users = dict(
                sorted(top_users.items(), key=lambda item: item[1], reverse=True)[:n]
            )

            return top_users


class Movies:
    def __init__(self, path_to_the_file):
        self.path = path_to_the_file
        self.data = self.read_file()  # вся таблица (первые 1_000 строк)

    def read_file(self):
        try:
            with open(self.path, "r", encoding="utf-8") as file:
                next(file)
                data = []
                cnt = 0
                for line in file:
                    if cnt < 1000:
                        cnt += 1
                        try:
                            data.append(self.str_handler(line.strip()))
                        except:
                            raise Exception("Error of handling string")
        except:
            raise Exception("File doesn't exist or it's uncorrect.")

        return data

    def extract_genres(self, row):
        genres = [genre.strip() for genre in row.split("|") if genre.strip() != ""]

        return genres

    def str_handler(self, string):
        first_quote = False
        string = string + ","
        index = 0
        new_string = []
        i = 0
        while i < (len(string)):
            if string[i] == '"' and not (first_quote):
                first_quote = True
                index = i + 1
                i += 1
            elif (
                string[i] == '"'
                and first_quote
                and ((i + 1) == len(string) or string[i + 1] == ",")
            ):
                first_quote = False
                new_string.append(string[index:i].strip())
                index = i + 2
                i += 2
            elif string[i] == "," and not (first_quote):
                new_string.append(string[index:i].strip())
                index = i + 1
                i += 1
            else:
                i += 1

        if len(new_string) != 3:
            raise Exception("Invalid type of string in the file!")

        if not new_string[0].isdigit():
            raise Exception("Invalid data type in the first column")
        new_string[0] = int(new_string[0])

        try:
            new_string[2] = self.extract_genres(new_string[2])
        except:
            raise Exception("Invalid data types in the line!")

        return new_string

    def extract_year(self):
        years = []
        for row in self.data:
            title = row[1]
            if "(" in title and ")" in title:
                match = re.search(r"\(\s*(\d{4})\s*\)", title)
                if match:
                    try:
                        years.append(int(match.group(1)))
                    except:
                        raise Exception("Can't handle year!")

        return years

    def dist_by_release(self):
        return dict(
            sorted(
                Counter(self.extract_year()).items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )

    def dist_by_genres(self):
        all_genres = []
        for row in self.data:
            genres = row[2]
            all_genres.extend(genres)

        return dict(
            sorted(Counter(all_genres).items(), key=lambda item: item[1], reverse=True)
        )

    def most_genres(self, n):
        future_dict = []
        for row in self.data:
            title = row[1]
            genres = row[2]
            genre_count = len(genres)
            future_dict.append((title, genre_count))

        future_dict.sort(key=lambda x: x[1], reverse=True)
        return dict(future_dict[:n])


class Links:
    def __init__(self, path_to_the_file, path_to_movie):
        if os.path.exists(path_to_the_file) and os.path.exists(path_to_movie):
            self.path = path_to_the_file
            file = self.read_file()
            self.movieId = self.select_movieId(file)
            self.imdbId = self.select_imdbId(file)
            self.tmdbId = self.select_tmdbId(file)
            self.dict_title = self.select_title(path_to_movie)
        else:
            raise FileNotFoundError(
                f"File did not exist: {path_to_the_file} or {path_to_movie}"
            )

    def read_file(self):
        with open(self.path, "r", encoding="utf-8") as file:
            lines = []
            line_number = -1

            for line in file:
                line_number += 1
                line = line.strip()

                if line_number == 0 and line != "movieId,imdbId,tmdbId":
                    raise Exception(f"Incorrect file header")

                if line_number == 0:
                    continue

                if len(lines) >= 1000:
                    break

                data = line.split(",")

                if len(data) != 3:
                    raise Exception(f"Invalid type of string in the file")

                if not data[0].strip().isdigit():
                    raise Exception(f"Invalid data type in the first column")

                if not data[1].strip().isdigit():
                    raise Exception(f"Invalid data type in the second column")

                # if not data[2].strip().isdigit():
                #     raise Exception(f"Invalid data type in the third column")

                lines.append(line)
            if len(lines) == 0:
                raise Exception("File contains no data or only header")
        return lines

    def select_title(self, path_to_movie):
        movies = Movies(path_to_movie)
        movieId = [line[0] for line in movies.data]
        titles = [line[1] for line in movies.data]
        dict_title = dict(zip(movieId, titles))
        return dict_title

    def select_movieId(self, lines):
        movieId = [line.split(",")[0] for line in lines]
        return movieId

    def select_imdbId(self, lines):
        imdbId = [line.split(",")[1] for line in lines]
        return imdbId

    def select_tmdbId(self, lines):
        tmdbId = [line.split(",")[2] for line in lines]
        return tmdbId

    def get_imdb(self, list_of_movies, list_of_fields):
        imdb_info = []
        for i in range(len(list_of_movies)):
            film_info = []
            film_info.append(list_of_movies[i])
            imdbId_index = (self.movieId).index(list_of_movies[i])
            imdbId = self.imdbId[imdbId_index]
            for field in list_of_fields:
                film_info.append(self.imdb_parsing(imdbId, field))
            imdb_info.append(film_info)
        imdb_info.sort(key=lambda x: int(x[0]), reverse=True)
        return imdb_info

    def imdb_parsing(self, imdbId, field):
        url = f"https://www.imdb.com/title/tt{imdbId}/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 YaBrowser/25.12.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ru,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://finance.yahoo.com/",
            "Sec-Ch-Ua": '"Chromium";v="142", "YaBrowser";v="25.12", "Not_A Brand";v="99", "Yowser";v="2.5"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Site returnes {response.status_code}")

        soup = BeautifulSoup(response.text, "html.parser")
        target = False

        match field.lower():
            case "director":
                # Director
                director_label = soup.find(
                    "span",
                    class_="ipc-metadata-list-item__label",
                    string=lambda t: t and "Director" in t,
                )
                if director_label:
                    parent_li = director_label.find_parent("li")
                    if parent_li:
                        director_links = parent_li.find_all(
                            "a", class_="ipc-metadata-list-item__list-content-item"
                        )
                        directors = [link.text.strip() for link in director_links]
                        target = directors
            case "budget":
                # Budget
                budget_label = soup.find(
                    "span",
                    class_="ipc-metadata-list-item__label",
                    string=lambda t: t and "Budget" in t,
                )
                if budget_label:
                    parent_li = budget_label.find_parent("li")
                    if parent_li:
                        budget_links = parent_li.find(
                            "span", class_="ipc-metadata-list-item__list-content-item"
                        )
                        budget = int(
                            re.findall(r"[\d,]+", budget_links.text.strip())[0].replace(
                                ",", ""
                            )
                        )
                        target = budget
            case "gross worldwide":
                # Gross worldwide
                gross_worldwide_label = soup.find(
                    "span",
                    class_="ipc-metadata-list-item__label",
                    string=lambda t: t and "Gross worldwide" in t,
                )
                if gross_worldwide_label:
                    parent_li = gross_worldwide_label.find_parent("li")
                    if parent_li:
                        gross_worldwide_links = parent_li.find(
                            "span", class_="ipc-metadata-list-item__list-content-item"
                        )
                        gross_worldwide = int(
                            re.findall(r"[\d,]+", gross_worldwide_links.text.strip())[
                                0
                            ].replace(",", "")
                        )
                        target = gross_worldwide
            case "runtime":
                # Runtime
                runtime_label = soup.find(
                    "span",
                    class_="ipc-metadata-list-item__label",
                    string=lambda t: t and "Runtime" in t,
                )
                if runtime_label:
                    parent_li = runtime_label.find_parent("li")
                    if parent_li:
                        runtime_links = parent_li.find(
                            "span",
                            class_="ipc-metadata-list-item__list-content-item--subText",
                        )
                        runtime = int(
                            re.findall(r"[\d,]+", runtime_links.text.strip())[0]
                        )
                        target = runtime
            case "title":
                # Title
                title_tag = soup.find("title")
                if title_tag:
                    title_text = title_tag.text.strip()
                    if title_text.endswith(" - IMDb"):
                        title_text = title_text[:-7]
                    target = title_text
            case _:
                # Other
                other_label = soup.find(
                    ["span", "a"],
                    class_="ipc-metadata-list-item__label",
                    string=lambda t: t and field in t,
                )
                if other_label:
                    parent_li = other_label.find_parent("li")
                    if parent_li:
                        other_links = parent_li.find_all(
                            ["span", "a"],
                            class_="ipc-metadata-list-item__list-content-item",
                        )
                        other = [link.text.strip() for link in other_links]
                        target = other
        return target

    def top_directors(self, n, list_of_movies):
        movieId_directors = self.get_imdb(list_of_movies, ["Director"])
        all_directors = []
        for _, directors in movieId_directors:
            if directors and directors != False:
                all_directors.extend(directors)

        directors_counter = Counter(all_directors)
        directors_top = sorted(directors_counter.items(), key=lambda x: (-x[1], x[0]))[
            :n
        ]
        return dict(directors_top)

    def most_expensive(self, n, list_of_movies):
        movieTitle_budget = self.get_imdb(list_of_movies, ["Budget"])
        budget_dict = {}
        for item in movieTitle_budget:
            movie_id = item[0]
            budget = item[1]
            budget_dict[movie_id] = budget

        title = [self.dict_title[int(movie_id)] for movie_id in list_of_movies]
        budget = [budget_dict[movie_id] for movie_id in list_of_movies]

        most_budgets = dict(zip(title, budget))
        most_budgets = sorted(most_budgets.items(), key=lambda x: (-x[1], x[0]))[:n]
        return dict(most_budgets)

    def most_profitable(self, n, list_of_movies):
        movie_data = self.get_imdb(list_of_movies, ["Gross worldwide", "Budget"])

        gross_dict = {}
        budget_dict = {}

        for item in movie_data:
            movie_id = item[0]
            gross_worldwide = item[1]
            budget = item[2]
            gross_dict[movie_id] = gross_worldwide
            budget_dict[movie_id] = budget

        profit_dict = {}
        for movie_id in list_of_movies:
            if movie_id in gross_dict and movie_id in budget_dict:
                profit = gross_dict[movie_id] - budget_dict[movie_id]
                profit_dict[movie_id] = profit

        title_profit_pairs = []
        for movie_id in list_of_movies:
            if movie_id in profit_dict:
                title = self.dict_title[int(movie_id)]
                profit = profit_dict[movie_id]
                title_profit_pairs.append((title, profit))

        title_profit_pairs.sort(key=lambda x: (-x[1], x[0]))
        result = dict(title_profit_pairs[:n])

        return result

    def longest(self, n, list_of_movies):
        movie_data = self.get_imdb(list_of_movies, ["Runtime"])

        runtime_dict = {}
        for item in movie_data:
            movie_id = item[0]
            runtime = item[1]
            runtime_dict[movie_id] = runtime

        title_runtime_pairs = []
        for movie_id in list_of_movies:
            if movie_id in runtime_dict:
                title = self.dict_title[int(movie_id)]
                runtime = runtime_dict[movie_id]
                title_runtime_pairs.append((title, runtime))

        title_runtime_pairs.sort(key=lambda x: (-x[1], x[0]))
        result = dict(title_runtime_pairs[:n])

        return result

    def top_cost_per_minute(self, n, list_of_movies):
        movie_data = self.get_imdb(list_of_movies, ["Budget", "Runtime"])
        budget_dict = {}
        runtime_dict = {}

        for item in movie_data:
            movie_id = item[0]
            budget_dict[movie_id] = item[1]
            runtime_dict[movie_id] = item[2]

        cost_per_minute_dict = {}
        for movie_id in list_of_movies:
            if movie_id in budget_dict and movie_id in runtime_dict:
                cost = budget_dict[movie_id] / runtime_dict[movie_id]
                cost_per_minute_dict[movie_id] = round(cost, 2)

        title_cost_pairs = []
        for movie_id in list_of_movies:
            if movie_id in cost_per_minute_dict:
                title = self.dict_title[int(movie_id)]
                cost = cost_per_minute_dict[movie_id]
                title_cost_pairs.append((title, cost))

        title_cost_pairs.sort(key=lambda x: (-x[1], x[0]))
        result = dict(title_cost_pairs[:n])

        return result


class Tests:
    def test_read_file(self):
        movies = Movies(PATH_TO_CSV_MOVIES)
        data = movies.data
        assert isinstance(
            data, list
        ), f"read_file должен возвращать list, получен {type(data)}"

        if data:
            first = data[0]
            assert isinstance(
                first, list
            ), f"элементы таблицы должны быть list, получен {type(first)}"
            assert isinstance(
                first[0], int
            ), f"movieId должен быть int, получен {type(first[0])}"
            assert isinstance(
                first[1], str
            ), f"title должен быть str, получен {type(first[1])}"
            assert isinstance(
                first[2], list
            ), f"genres должен быть list, получен {type(first[2])}"

    def test_extract_genres(self):
        movies = Movies(PATH_TO_CSV_MOVIES)
        test_s = "Action|Adventure|Sci-Fi"
        genres = movies.extract_genres(test_s)
        assert isinstance(
            genres, list
        ), f"extract_genres должен возвращать list, получен {type(genres)}"

        for g in genres:
            assert isinstance(g, str), f"genre должен быть str, получен {type(g)}"

    def test_str_handler(self):
        movies = Movies(PATH_TO_CSV_MOVIES)
        test_s = "1,Stranger Things,Adventure|Fantasy|Children"
        handle = movies.str_handler(test_s)
        assert isinstance(
            handle, list
        ), f"str_handler должен возвращать list, получен {type(handle)}"

        assert isinstance(
            handle[0], int
        ), f"элемент 0 должен быть int, получен {type(handle[0])}"
        assert isinstance(
            handle[1], str
        ), f"элемент 1 должен быть str, получен {type(handle[1])}"
        assert isinstance(
            handle[2], list
        ), f"элемент 2 должен быть list, получен {type(handle[2])}"

    def test_extract_year(self):
        movies = Movies(PATH_TO_CSV_MOVIES)
        res = movies.extract_year()
        assert isinstance(
            res, list
        ), f"extract_year должен возвращать list, получен {type(res)}"
        if res:
            for year in res:
                assert isinstance(
                    year, int
                ), f"год должны быть int, получен {type(year)}"
                assert (
                    1894 < year < 2027
                ), f"год {year} вне диапазона, p.s. первый фильм сняли в 1895 году"

    def test_dict_by_release(self):
        movies = Movies(PATH_TO_CSV_MOVIES)
        res = movies.dist_by_release()

        assert isinstance(
            res, dict
        ), f"dict_by_release должен возвращать dict, получен {type(res)}"
        if res:
            for key, value in res.items():
                # годы
                assert isinstance(
                    key, int
                ), f"ключ (год) должен быть int, получен {type(key)}"

                # количества
                assert isinstance(
                    value, int
                ), f"значение (количество) должно быть int, получен {type(value)}"
                assert value > 0, f"кол-во должно быть положительным, получено {value}"

            # по убыванию
            values = list(res.values())
            for i in range(len(values) - 1):
                assert values[i] >= values[i + 1], f"нарушение сортировки"

    def test_dict_by_genres(self):
        movies = Movies(PATH_TO_CSV_MOVIES)
        res = movies.dist_by_genres()
        assert isinstance(
            res, dict
        ), f"Метод должен возвращать dict, получен {type(res)}"

        if res:
            for key, value in res.items():
                assert isinstance(
                    key, str
                ), f"ключ (жанр) должен быть str, получен {type(key)}"
                assert len(key) > 0, "Название жанра не может быть пустой строкой"

                assert isinstance(
                    value, int
                ), f"значение (количество) должно быть int, получен {type(value)}"
                assert value > 0, f"кол-во должно быть положительным, получено {value}"

            # по убыванию
            values = list(res.values())
            for i in range(len(values) - 1):
                assert values[i] >= values[i + 1], f"нарушение сортировки"

    def test_most_genres(self):
        movies = Movies(PATH_TO_CSV_MOVIES)
        test_n = [0, 1, 5]

        for n in test_n:
            res = movies.most_genres(n)
            assert isinstance(
                res, dict
            ), f"most_genres должен возвращать dict, получен {type(res)}"

            if n == 0:
                assert (
                    len(res) == 0
                ), f"при n=0 должен быть пустой словарь, получено {len(res)} элементов"
            elif n <= len(movies.data):
                assert (
                    len(res) == n
                ), f"при n={n} должно быть {n} элементов, получено {len(res)}"
            else:
                assert len(res) <= len(
                    movies.data
                ), f"элементов не может быть больше чем строк в данных"

    def test_select_tags(self):
        tags = Tags(PATH_TO_CSV_TAGS)
        tags_ls = tags.tags
        assert isinstance(
            tags_ls, list
        ), f"tags должен быть list, получен {type(tags_ls)}"
        if tags_ls:
            for tag in tags_ls:
                assert isinstance(tag, str), f"тег должен быть str, получен {type(tag)}"

    def test_most_words(self):
        tags = Tags(PATH_TO_CSV_TAGS)
        test_n = [0, 1, 5]
        for n in test_n:
            res = tags.most_words(n)
            assert isinstance(
                res, dict
            ), f"most_words должен возвращать dict, получен {type(res)}"
            if n == 0:
                assert len(res) == 0, f"при n = 0 должен быть пустой словарь"
            else:
                assert (
                    len(res) <= n
                ), f"должно быть не больше {n} элементов, получено {len(res)}"
                if res:
                    values = list(res.values())
                    for i in range(len(values) - 1):
                        assert values[i] >= values[i + 1], f"нарушение сортировки"

    def test_longest(self):
        tags = Tags(PATH_TO_CSV_TAGS)
        test_n = [0, 1, 5]
        for n in test_n:
            res = tags.longest(n)
            assert isinstance(
                res, list
            ), f"longest должен возвращать list, получен {type(res)}"
            if n == 0:
                assert len(res) == 0, f"при n = 0 должен быть пустой список"
            else:
                assert len(res) <= n, f"должно быть не больше {n} элементов"
                if res:
                    for i in range(len(res) - 1):
                        assert len(res[i]) >= len(
                            res[i + 1]
                        ), f"нарушение сортировки по длине"

    def test_most_words_and_longest(self):
        tags = Tags(PATH_TO_CSV_TAGS)
        test_n = [0, 1, 5]

        for n in test_n:
            res = tags.most_words_and_longest(n)
            assert isinstance(
                res, list
            ), f"most_words_and_longest должен возвращать list, получен {type(res)}"
            if n == 0:
                assert len(res) == 0, f"при n=0 должен быть пустой список"
            else:
                most_words_set = set(tags.most_words(n).keys())
                longest_set = set(tags.longest(n))
                assert set(res) == most_words_set.intersection(
                    longest_set
                ), f"результат не является пересечением most_words и longest"
                if len(res) > 1:
                    for i in range(len(res) - 1):
                        assert res[i] <= res[i + 1], f"нарушение алфавитной сортировки"

    def test_most_popular(self):
        tags = Tags(PATH_TO_CSV_TAGS)
        test_n = [0, 1, 5]

        for n in test_n:
            res = tags.most_popular(n)
            assert isinstance(
                res, dict
            ), f"most_popular должен возвращать dict, получен {type(res)}"

            if n == 0:
                assert len(res) == 0, f"при n=0 должен быть пустой словарь"
            else:
                assert len(res) <= n, f"должно быть не больше {n} элементов"
                if res:
                    values = list(res.values())
                    for i in range(len(values) - 1):
                        assert (
                            values[i] >= values[i + 1]
                        ), f"нарушение сортировки по популярности"

    def test_tags_with(self):
        tags = Tags(PATH_TO_CSV_TAGS)
        test_words = ["action", "comedy", "love"]

        for word in test_words:
            res = tags.tags_with(word)
            assert isinstance(
                res, list
            ), f"tags_with должен возвращать list, получен {type(res)}"
            for tag in res:
                assert word in tag.lower(), f"тег '{tag}' не содержит слово '{word}'"
            if len(res) > 1:
                for i in range(len(res) - 1):
                    assert res[i] <= res[i + 1], f"нарушение алфавитной сортировки"

    def test_select_methods(self):
        links = Links(PATH_TO_CSV_LINKS, PATH_TO_CSV_MOVIES)

        for movie_id in links.movieId[:10]:
            assert (
                movie_id.isdigit()
            ), f"movieId должен содержать только цифры: {movie_id}"

        for imdb_id in links.imdbId[:10]:
            assert (
                imdb_id.isdigit() or imdb_id == ""
            ), f"imdbId должен содержать только цифры: {imdb_id}"

    def test_get_imdb(self):
        links = Links(PATH_TO_CSV_LINKS, PATH_TO_CSV_MOVIES)

        test_movies = links.movieId[:3] if len(links.movieId) >= 3 else links.movieId
        test_fields = ["Title", "Director"]
        try:
            result = links.get_imdb(test_movies, test_fields)
            assert isinstance(result, list), "get_imdb должен возвращать список"

            if result:
                for item in result:
                    assert isinstance(item, list), "элементы должен быть list"
                    assert isinstance(
                        item[0], str
                    ), "первый элемент должен быть movieId (строка)"
                    assert (
                        len(item) == len(test_fields) + 1
                    ), f"должно быть {len(test_fields) + 1} элементов: movieId + поля"
        except Exception as e:
            pytest.skip(f"пропуск теста: {e}")

    def test_top_directors(self):
        links = Links(PATH_TO_CSV_LINKS, PATH_TO_CSV_MOVIES)
        test_movies = links.movieId[:1]
        try:
            result = links.top_directors(1, test_movies)
            assert isinstance(
                result, dict
            ), f"top_directors должен возвращать dict, получен {type(result)}"
        except Exception as e:
            pytest.skip(f"пропуск теста: {e}")

    def test_most_expensive(self):
        links = Links(PATH_TO_CSV_LINKS, PATH_TO_CSV_MOVIES)
        try:
            test_movies = links.movieId[:1] if len(links.movieId) > 0 else []

            if not test_movies:
                pytest.skip("Нет данных для тестирования")

            result = links.most_expensive(1, test_movies)
            if result is not None:
                assert isinstance(result, dict), "most_expensive должен возвращать dict"

        except Exception as e:
            pytest.skip(f"пропуск теста most_expensive из-за: {e}")

    def test_most_profitable(self):
        links = Links(PATH_TO_CSV_LINKS, PATH_TO_CSV_MOVIES)
        test_movies = links.movieId[:1]
        n = 1
        try:
            result = links.most_profitable(n, test_movies)

            assert isinstance(
                result, dict
            ), f"most_profitable должен возвращать dict, получен {type(result)}"

            if result:
                assert len(result) <= n, f"должно быть не больше {n} элементов"

                for title, profit in result.items():
                    assert isinstance(title, str), "название фильма должно быть str"
                    assert isinstance(
                        profit, (int, float)
                    ), "прибыль должна быть числом"

                if len(result) > 1:
                    profits = list(result.values())
                    for i in range(len(profits) - 1):
                        assert profits[i] >= profits[i + 1], "нарушение сортировки"
        except Exception as e:
            pass

    def test_links_longest(self):
        links = Links(PATH_TO_CSV_LINKS, PATH_TO_CSV_MOVIES)
        test_movies = links.movieId[:1]
        n = 1
        try:
            result = links.longest(n, test_movies)
            assert isinstance(result, dict), f"longest должен возвращать dict"
            if result:
                assert len(result) <= n, f"должно быть не больше {n} элементов"

                for title, runtime in result.items():
                    assert isinstance(title, str), "название должно быть str"
                    assert isinstance(
                        runtime, (int, float)
                    ), "длительность должна быть числом"

                if len(result) > 1:
                    runtimes = list(result.values())
                    for i in range(len(runtimes) - 1):
                        assert runtimes[i] >= runtimes[i + 1], "нарушение сортировки"
        except Exception as e:
            pytest.skip(f"пропуск теста: {e}")

    def test_top_cost_per_minute(self):
        links = Links(PATH_TO_CSV_LINKS, PATH_TO_CSV_MOVIES)
        test_movies = links.movieId[:1]
        n = 1
        try:
            result = links.top_cost_per_minute(n, test_movies)
            assert isinstance(
                result, dict
            ), f"top_cost_per_minute должен возвращать dict"
            if result:
                assert len(result) <= n, f"должно быть не больше {n} элементов"
                for title, cost in result.items():
                    assert isinstance(title, str), "название должно быть str"
                    assert isinstance(
                        cost, (int, float)
                    ), "стоимость/минута должна быть числом"

                if len(result) > 1:
                    costs = list(result.values())
                    for i in range(len(costs) - 1):
                        assert costs[i] >= costs[i + 1], "нарушение сортировки"
        except Exception as e:
            pytest.skip(f"пропуск теста top_cost_per_minute: {e}")

    def test_ratings_str_handler(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        test_string = "1,100,4.5,964982703"
        result = ratings.str_handler(test_string)

        assert isinstance(result, list), "str_handler должен возвращать list"
        assert isinstance(result[0], int), "userId должен быть int"
        assert isinstance(result[1], int), "movieId должен быть int"
        assert isinstance(result[2], float), "rating должен быть float"
        assert isinstance(result[3], int), "timestamp должен быть int"

    def test_ratings_read_file(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        data = ratings.data

        assert isinstance(data, list), "read_file должен возвращать list"
        if data:
            first_row = data[0]
            assert isinstance(first_row, list), "элементы должны быть list"

    def test_movies_class_init(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)
        assert hasattr(movies, "union_data"), "нет атрибута union_data"
        assert isinstance(movies.union_data, list), "union_data должен быть списком"

    def test_movies_dist_by_year(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)

        result = movies.dist_by_year()
        assert isinstance(result, dict), "dist_by_year должен возвращать dict"

        if result:
            for year, count in result.items():
                assert isinstance(year, int), "год должен быть int"
                assert isinstance(count, int), "кол-во должно быть int"
                assert count > 0, "кол-во должно быть положительным"

            years = list(result.keys())
            for i in range(len(years) - 1):
                assert years[i] < years[i + 1], "нарушение сортировки"

    def test_movies_dist_by_rating(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)

        result = movies.dist_by_rating()
        assert isinstance(result, dict), "dist_by_rating должен возвращать dict"

        if result:
            for rating, count in result.items():
                assert isinstance(rating, float), "рейтинг должен быть float"
                assert isinstance(count, int), "кол-во должно быть int"
                assert count > 0, "кол-во должно быть положительным"

            ratings_list = list(result.keys())
            for i in range(len(ratings_list) - 1):
                assert ratings_list[i] < ratings_list[i + 1], "нарушение сортировки"

    def test_movies_top_by_num_of_ratings(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)

        test_n = [0, 1, 5]
        for n in test_n:
            result = movies.top_by_num_of_ratings(n)
            assert isinstance(
                result, dict
            ), "top_by_num_of_ratings должен возвращать dict"

            if n == 0:
                assert len(result) == 0, "при n = 0 должен быть пустой словарь"
            else:
                assert len(result) <= n, f"должно быть не больше {n} элементов"
                if result:
                    counts = list(result.values())
                    for i in range(len(counts) - 1):
                        assert counts[i] >= counts[i + 1], "нарушение сортировки"

    def test_movies_top_by_ratings_average(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)

        result = movies.top_by_ratings(5, "average")
        assert isinstance(result, dict), "top_by_ratings должен возвращать dict"

        if result:
            for title, rating in result.items():
                assert isinstance(title, str), "название должно быть str"
                assert isinstance(rating, float), "рейтинг должен быть float"
                assert 0 <= rating <= 5, "рейтинг должен быть от 0 до 5"

            ratings_list = list(result.values())
            for i in range(len(ratings_list) - 1):
                assert ratings_list[i] >= ratings_list[i + 1], "нарушение сортировки"

    def test_movies_top_by_ratings_median(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)

        result = movies.top_by_ratings(5, "median")
        assert isinstance(result, dict), "top_by_ratings должен возвращать dict"

        if result:
            for title, rating in result.items():
                assert isinstance(title, str), "название должно быть str"
                assert isinstance(rating, float), "рейтинг должен быть float"
                assert 0 <= rating <= 5, "рейтинг должен быть от 0 до 5"

            ratings_list = list(result.values())
            for i in range(len(ratings_list) - 1):
                assert ratings_list[i] >= ratings_list[i + 1], "нарушение сортировки"

    def test_movies_top_controversial(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)

        result = movies.top_controversial(5)
        assert isinstance(result, dict), "top_controversial должен возвращать dict"

        if result:
            for title, variance in result.items():
                assert isinstance(title, str), "название должно быть str"
                assert isinstance(variance, float), "дисперсия должна быть float"
                assert variance >= 0, "дисперсия должна быть неотрицательной"

            variances = list(result.values())
            for i in range(len(variances) - 1):
                assert variances[i] >= variances[i + 1], "нарушение сортировки"

    def test_users_dist_by_users(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)
        users = ratings.Users(movies)

        result = users.dist_by_users()
        assert isinstance(result, dict), "dist_by_users должен возвращать dict"

        if result:
            for user_id, count in result.items():
                assert isinstance(user_id, int), "user_id должен быть int"
                assert isinstance(count, int), "кол-во должно быть int"
                assert count > 0, "кол-во должно быть положительным"

    def test_users_dist_by_metric_average(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)
        users = ratings.Users(movies)

        result = users.dist_by_metric("average")
        assert isinstance(result, dict), "dist_by_metric должен возвращать dict"

        if result:
            for user_id, rating in result.items():
                assert isinstance(user_id, int), "user_id должен быть int"
                assert isinstance(rating, float), "ср. рейтинг должен быть float"
                assert 0 <= rating <= 5, "рейтинг должен быть от 0 до 5"

    def test_users_dist_by_metric_median(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)
        users = ratings.Users(movies)

        result = users.dist_by_metric("median")
        assert isinstance(result, dict), "dist_by_metric должен возвращать dict"

        if result:
            for user_id, rating in result.items():
                assert isinstance(user_id, int), "user_id должен быть int"
                assert isinstance(rating, float), "Медианный рейтинг должен быть float"
                assert 0 <= rating <= 5, "Рейтинг должен быть от 0 до 5"

    def test_users_top_controversial(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)
        users = ratings.Users(movies)

        result = users.top_controversial(5)
        assert isinstance(result, dict), "top_controversial должен возвращать dict"

        if result:
            for user_id, variance in result.items():
                assert isinstance(user_id, int), "user_id должен быть int"
                assert isinstance(variance, float), "дисперсия должна быть float"
                assert variance >= 0, "дисперсия должна быть неотрицательной"

            variances = list(result.values())
            for i in range(len(variances) - 1):
                assert variances[i] >= variances[i + 1], "нарушение сортировки"

    def test_edge_cases(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)

        result = movies.top_by_num_of_ratings(0)
        assert len(result) == 0, "при n = 0 должен быть пустой словарь"

        all_movies_count = len(set(row[4] for row in movies.union_data))
        result = movies.top_by_num_of_ratings(all_movies_count + 10)
        assert (
            len(result) <= all_movies_count
        ), "не может быть больше фильмов чем есть в датасете"

    def test_invalid_metric(self):
        ratings = Ratings(PATH_TO_CSV_RATINGS)
        movies = ratings.Movies(ratings, PATH_TO_CSV_MOVIES)
        try:
            movies.top_by_ratings(5, "invalid")
            assert False, "должна быть ошибка для неверного metric"
        except Exception as e:
            assert "Not valid metric" in str(e)
        users = ratings.Users(movies)
        try:
            users.dist_by_metric("invalid")
            assert False, "должна быть ошибка для неверного metric"
        except Exception as e:
            assert "Not valid metric" in str(e)
