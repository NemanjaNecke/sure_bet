last = driver.find_elements(By.CSS_SELECTOR, '.ps-content>div:last-of-type')
driver.execute_script(open('./scroll.js').read())
document.querySelector('.overview-content>.ps.ps--active-y>.ps-content>div:last-of-type').scrollIntoView(true,{behavior: 'smooth', block: 'end', inline: 'nearest'})