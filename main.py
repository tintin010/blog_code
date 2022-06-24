import bs4
import urllib.request
import re
import pymysql
from datetime import datetime


def open_db():
    conn = pymysql.connect(host="localhost", port=3306, user="root", password="", db="movie")

    cur = conn.cursor(pymysql.cursors.DictCursor)

    return conn, cur


def close_db(conn, cur):
    cur.close()
    conn.close()

url = "https://movie.naver.com/movie/running/current.naver#"
html = urllib.request.urlopen(url)
soup = bs4.BeautifulSoup(html, 'html.parser')
movielist = soup.find("ul",{"class":"lst_detail_t1"})
lis = movielist.findAll("li")
m_title = []    #영화 제목이 들어갈 리스트
m_rate = []     #영화 평점이 들어갈 리스트
netizen_rate = []   #네티즌 평점이 들어갈 리스트
netizen_count = []  #네티즌 평점 참여자 수 들어갈 리스트
journalist_score = []   #기자평론가 평점 리스트
journalist_count = []   #기자 평론가 참여자수 리스트
scope = []  #개요 리스트
playing_time = []
opening_date = []   # 개봉날짜 들어갈 리스트
p_t = []    # 상영시간 들어갈 리스트
director = []   # 감독 들어갈 리스트
image = []  # 이미지 주소 들어갈 리스트
n = len(lis)
index = -1
jcnt=0
ncnt=0
jcnt2=0
for li in lis:
    index+=1
    # 영화 등급
    grade = li.find("dt",{"class":"tit"})
    # print(grade_name)
    if(grade.find("span")):
        grade_f = grade.find("span")
        m_rate.append(grade_f.text)
    else:
        m_rate.append("NULL")
    # print(m_rate)

    # 영화 제목
    name = li.find("dt", {"class": "tit"})
    name_f = name.find("a")
    m_title.append(name_f.text)

    # 네티즌 평점
    # 네티즌 인원
    if(li.find("dt", {"class": "tit_t1"})):
        n_grade = li.find("div", {"class": "star_t1"})
        n_grade_2 = n_grade.find("span", {"class": "num"})
        netizen_rate.append(n_grade_2.text)

        n_cnt = n_grade.find("span", {"class": "num2"})
        n_cnt_2 = n_cnt.find("em")
        netizen_count.append(n_cnt_2.text)
        ncnt+=1
        # print(netizen_count)
    else:
        netizen_rate.append("NULL")
        netizen_count.append("NULL")

    # 기자평론가 인원
    # 기자평론가 평점

    j_grade = li.findAll("div", {"class": "star_t1"})
    if(len(j_grade)==2):
        j_grade_2 = j_grade[1].find("span", {"class": "num"})
        journalist_score.append(n_grade_2.text)

        j_cnt = j_grade[1].find("span", {"class": "num2"})
        j_cnt_2 = j_cnt.find("em")
        journalist_count.append(j_cnt_2.text)
        # print(journalist_count)
        jcnt+=1
    else:
        journalist_score.append("NULL")
        journalist_count.append("NULL")
        jcnt2+=1

    # 개요
    # 감독
    sc = li.find("dl", {"class": "info_txt1"})
    sc_2 = sc.findAll("dd")
    sctime_cnt=0;
    for dd in sc_2:
        sctime_cnt +=1
        if(sctime_cnt == 1):
            str = ""
            sc_cnt = 0
            sc_3 = dd.findAll("a")
            for a in sc_3:
                if sc_cnt == 0:
                    str += a.text
                else:
                    str += "," + a.text
                sc_cnt += 1
            scope.append(str)
        elif sctime_cnt == 2:
            str = dd.find("a").text
            director.append(str)

    # #상영시간
    m_time = li.find("dl", {"class": "info_txt1"})
    m_time_2 = m_time.findAll("dd")
    for tim in m_time_2:
        # print(tim.text)
        str=""
        while tim.find('span'):
            tim.find("span").decompose()
            str = tim.text.strip()
            str = re.sub(r'\n', '', str)
            str = re.sub(r'\t', '', str)
            str = re.sub(r'\r', '', str)
        if (len(str) == 0):
            continue
        playing_time.append(str)
    # 영화 대표 이미지
    img = li.find("div", {"class": "thumb"})
    img_2 = img.find("img")
    imgsrc = img_2.get("src")
    image.append(imgsrc)


for pt in playing_time:
    pt_l = pt.split("분")
    p_t.append(pt_l[0] + "분")
    opening_date.append(pt_l[1])

n = len(playing_time)

def moviefinal():

    conn, cur = open_db()
    conn2, cur2 = open_db()
    sql = """
        create table movie
        (
            
            title varchar(100),
            movie_rate varchar(30),
            netizen_rate float(7,2),
            netizen_count integer,
            journalist_score float(7,2),
            journalist_count integer,
            scope varchar(1000),
            playing_time varchar(100),
            opening_date varchar(100),
            director varchar(100),
            image varchar(100),
            enter_date varchar(100)
        );
        """
    cur.execute(sql)

    insert_sql = """insert into movie(title, movie_rate, netizen_rate,
             netizen_count, journalist_score,journalist_count, scope, playing_time,opening_date, director, image, enter_date)
                        values(%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"""

    buffer = []

    for tit, m_r, n_r, n_c, j_s, j_c,sc, pl_t, op_d, dir, img in zip(m_title, m_rate, netizen_rate, netizen_count, journalist_score, journalist_count,scope, p_t, opening_date, director, image):
        now = datetime.now()
        n_time = now.strftime('%Y-%m-%d %H:%M:%S')  #현재 시간을 파악하여 enter_time 튜플에 저장
        t = (tit, m_r, n_r, n_c, j_s, j_c,sc, pl_t, op_d, dir, img, n_time)
        buffer.append(t)

        if len(buffer) % 10 == 0:
            cur.executemany(insert_sql, buffer)
            conn.commit()
            buffer = []

    if buffer:
        cur.executemany(insert_sql, buffer)
        conn.commit()

    close_db(conn, cur)

if __name__ == '__main__':
    moviefinal()