import json

from app.netflix_worker import NetflixWorker
from flask import Flask

DATABASE_PATH = './res/netflix.db'

app = Flask('__main__')


def make_code(data):
    return f'<pre>{json.dumps(data, indent=4)}</pre>'


@app.route('/movie/<string:title>')
def movie_info_view(title):
    with NetflixWorker(DATABASE_PATH) as worker:
        row = worker.get_by_title(title)
        return make_code(row)


@app.route('/movie/<path:path>')
def between_release_year_view(path):
    bounds = path.split('/')[::2]

    if len(bounds) != 2 or not bounds[0].isdigit() or not bounds[1].isdigit():
        return 'Ошибка ввода запроса. Нужно /movie/year/to/year'

    left, right = bounds

    with NetflixWorker(DATABASE_PATH) as worker:
        rows = worker.get_between_list('release_year', left, right, limit=100)
        return make_code(rows)


@app.route('/movie/children')
def pg_children_view():
    with NetflixWorker(DATABASE_PATH) as worker:
        rows = worker.get_rating_list(['G'], limit=100)
        return make_code(rows)


@app.route('/movie/family')
def pg_family_view():
    with NetflixWorker(DATABASE_PATH) as worker:
        rows = worker.get_rating_list(['G', 'PG', 'PG-13'], limit=100)
        return make_code(rows)


@app.route('/movie/adult')
def pg_adult_view():
    with NetflixWorker(DATABASE_PATH) as worker:
        rows = worker.get_rating_list(['R', 'NC-17'], limit=100)
        return make_code(rows)


@app.route('/genre/<string:genre>')
def genre_view(genre):
    with NetflixWorker(DATABASE_PATH) as worker:
        rows = worker.get_genre_list(genre, limit=10)
        return make_code(rows)


def main():
    with NetflixWorker(DATABASE_PATH) as worker:
        print(worker.get_actor_pair_list('Rose McIver', 'Ben Lamb'))
        print(worker.search('Movie', 2010, 'Horror'))

    app.run(debug=True)


if __name__ == '__main__':
    main()
