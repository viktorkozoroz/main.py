from selenium import webdriver
import pickle
import time
from webdriver_manager.chrome import ChromeDriverManager

#--------------------------------------установка драйверов---------------------------------------------------------
browser = webdriver.Chrome(ChromeDriverManager().install())

#мы заходим на сайт wb
browser.get('https://seller.wildberries.ru/analytics')

#скрипт останавливается на 100 секунд. За эти 100 секунд мы должны вставить номер телефона, получить смс, и вставить код из смс и войти в лк
time.sleep(100)

#скрипт идет дальше, из браузера забирает куки файлы и сохраняет их в папку(path)
path = r'C:\Users\v.li\Desktop\cookie\session'
pickle.dump(browser.get_cookies(), open(path, 'wb'))

print('куки сохранены')

