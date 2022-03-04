from flask import Flask, render_template, g, request, flash, redirect, url_for
import sqlite3
import os
from FDataBase import FDataBase
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, login_user, logout_user, login_required
from UserLogin import UserLogin

DATABASE = '/tmp/flsite.db'
DEBUG = True
SECRET_KEY = 'jfdlgjshfdlgjhsfdg'

app = Flask(__name__)
login_manager = LoginManager(app)
app.config['SECRET_KEY'] = 'dfgqwerg98a9er1v9a8aklyi8467948ga9348gz998aemn,498qs91b6z3z798'
app.config.from_object(__name__)

app.config.update(dict(DATABASE=os.path.join(app.root_path, 'flsite.db')))

dbase = None


@app.before_request
def before_request():
    """Установление соединения с БД перед выполнением запроса"""
    global dbase
    db = get_db()
    dbase = FDataBase(db)


def connect_db():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn


def create_db():
    db = connect_db()
    with app.open_resource('sq_db.sql', mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()
    db.close()


def get_db():
    if not hasattr(g, 'link_db'):
        g.link_db = connect_db()
    return g.link_db


@login_manager.user_loader
def load_user(user_id):
    print("load_user")
    return UserLogin().fromDB(user_id, dbase)


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'link_db'):
        g.link_db.close()


@app.route("/")
def index():
    return render_template("index.html", menu=dbase.getMenu())


@app.route("/reg", methods=["POST", "GET"])
def reg():
    if request.method == "POST":
        if len(request.form['name']) > 1 and len(request.form['email']) > 1 \
                and len(request.form['psw']) > 1 and request.form['psw'] == request.form['psw2']:
            hash = generate_password_hash(request.form['psw'])
            res = dbase.addUser(request.form['name'], request.form['email'], hash)
            if res:
                flash("Вы успешно зарегистрированы", "success")
                return redirect(url_for('login'))
            else:
                flash("Ошибка при добавлении в БД", "error")
        else:
            flash("Неверно заполнены поля", "error")

    return render_template("reg.html", title="Регистрация")


@app.route("/login", methods=["POST", "GET"])
def login():
    if request.method == "POST":
        user = dbase.getUserByEmail(request.form['email'])
        if user and check_password_hash(user['psw'], request.form['psw']):
            userlogin = UserLogin().create(user)
            login_user(userlogin)
            return redirect(url_for('addPost'))

        flash("Неверная пара логин/пароль", "error")

    return render_template("login.html")

@login_manager.unauthorized_handler
def unauthorized_callback():
    return redirect('/login')

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect('reg')

@login_required
@app.route("/add_post", methods=["POST", "GET"])
def addPost():
    if request.method == "POST":
        res = dbase.addPost(request.form['amount'],
                            request.form['description'],
                            request.form['date'],
                            request.form['who'])
        if not res:
            flash('Ошибка добавления статьи', category='error')
        else:
            flash('Статья добавлена успешно', category='success')
    return render_template('add_post.html', posts=dbase.getPosts(), title="Добавление статьи")


@app.route("/post/<int:id_post>")
def showPost(id_post):
    title, post = dbase.getPost(id_post)
    if not title:
        abort(404)

    return render_template('post.html', menu=dbase.getMenu(), title=title, post=post)


if __name__ == '__main__':
    app.run(debug=True)
