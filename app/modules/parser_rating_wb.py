
def get_rating(arts):
    for i in arts:
        url = f"https://www.wildberries.ru/catalog/{str(i)}/detail.aspx?targetUrl=IN"
        response = requests.get(url)
        soup = BeautifulSoup(response.text)

        rating[i] = soup.find('span', {'data-link': 'text{: product^star}'})
        print(rating[i])

        review_count[i] = soup.find('span', {'data-link': "{include tmpl='productCardCommentsCount'}"})
        print(review_count[i])
        time.sleep(self.time_wait)

    return rating, review_count

@app.route('/parser-rating-wb', methods=['GET', 'POST'])
def upload_file():
    """Обработка файла excel  - шапка нужна"""
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df = pd.read_excel(uploaded_files[0])
        arts = df["Номенклатура"].tolist()

        rating, review_count = get_rating(arts)

        d = {}
        good_value = []
        rating_value = []
        review_count_value = []

        for key, value in rating.items():

            good_value.append(key)
            try:
                rating_value.append(value.text)
            except BaseException:
                rating_value.append(value)

        d["Номенклатура"] = good_value
        d["Рейтинг"] = rating_value

        for key, value in review_count.items():
            # отсекаем слова от чисел с отзывами

            try:
                value = value.text.split()
                value = value[0]
                print(value)
                review_count_value.append(value.text)
            except BaseException:
                review_count_value.append(value)

        d["Кол-во отзывов"] = review_count_value

        df = pd.DataFrame(data=d)
        file_name = 'parser-rating-wb.xlsx'
        df.to_excel(file_name, index=False)

        return send_file(file_name, as_attachment=True)
    return render_template("parser_rating_wb_views.html")
