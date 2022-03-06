from flask import Flask, render_template, request, redirect
from flask_login import login_required, current_user, login_user, logout_user
from models import Company, UserModel, Post, Task, db, login
import datetime

app = Flask(__name__)
app.secret_key = 'xyz'

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
login.init_app(app)
login.login_view = 'login'


@app.before_first_request
def create_all():
    db.create_all()


@app.route('/blog', methods=['POST', 'GET'])
@login_required
def blog():
    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']
        if request.form['date'] == "": date = datetime.date.today()
        if not request.form['username']: username = "oooollll"
        post = Post(amount=amount, description=description, date=date, username=username)
        db.session.add(post)
        db.session.commit()

    posts = db.session.query(Post).all()
    return render_template('blog.html', posts=posts)


@app.route('/login', methods=['POST', 'GET'])
def login():
    if current_user.is_authenticated:
        return redirect('/blog')

    if request.method == 'POST':
        email = request.form['email']
        user = User.query.filter_by(email=email).first()
        if user is not None and user.check_password(request.form['password']):
            login_user(user)
            return redirect('/blog')

    return render_template('login.html')


@app.route('/company_register', methods=['POST', 'GET'])
def company_register():
    if request.method == 'POST':
        company_name = request.form['company_name']
        password = request.form['password']

        if Company.query.filter_by(company_name=company_name).first():
            return ('Email already Present')

        company = Company(company_name=company_name)
        company.set_password(password)
        db.session.add(company)
        db.session.commit()

        return redirect('/login')
    return render_template('company_register.html')


@app.route('/user_register', methods=['POST', 'GET'])
def user_register():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':

        user_name = request.form['user_name']
        user_password = request.form['user_password']

        if UserModel.query.filter_by(user_name=user_name).first():
            return ('User_name already Present')

        user = UserModel(user_name=user_name)
        user.set_password(user_password)
        db.session.add(user)
        db.session.commit()
        return redirect('/blog')
    return render_template('user_register.html')


@app.route('/logout')
def logout():
    logout_user()
    return redirect('/blog')


if __name__ == '__main__':
    app.run(host="localhost", port=8001, debug=True)
