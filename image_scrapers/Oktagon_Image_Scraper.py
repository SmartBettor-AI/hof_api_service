from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import urllib.request
import time
from unidecode import unidecode
import re

# Set your chromedriver path here
chromedriver_path = "/Users/michaelblackburn/Desktop/SMRT/HOF-Website/chromedriver-mac-arm64/chromedriver"

# Initialize the webdriver
service = Service(chromedriver_path)
driver = webdriver.Chrome(service=service)
driver.maximize_window()

# URL of the fighters page
url = "https://oktagonmma.com/en/fighters/"
driver.get(url)

# Wait until the 'Load more fighters' button is visible and clickable
wait = WebDriverWait(driver, 10)

# Function to click 'Load more fighters' until the button disappears
def load_all_fighters():
    def accept_cookies():
        try:
            # Wait for the 'Accept all' button to be present (if it exists)
            accept_button = wait.until(EC.presence_of_element_located((By.ID, "c-p-bn")))
            if accept_button.is_displayed():
                accept_button.click()
                print("Accepted cookies.")
            else:
                print("Cookie banner not displayed.")
        except:
            # If the button doesn't exist or can't be clicked, print an error
            print("No 'Accept all' button found or error occurred.")
    
    # Call the accept_cookies function once at the start
    accept_cookies()

    while True:
        try:
            # Scroll down the page a little bit before checking for the button
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # Allow time for the page to scroll and load new content

            # Before looking for the "Load more fighters" button, try to accept cookies again
            accept_cookies()

            # Wait until the 'Load more fighters' button is clickable
            load_more_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@type='button']//div[text()='Load more fighters']")))
            
            # Scroll specifically to the button in case it's hidden or not in view
            actions = ActionChains(driver)
            actions.move_to_element(load_more_button).perform()

            # Click the 'Load more fighters' button
            time.sleep(1)  # Small pause to ensure everything is ready before clicking
            load_more_button.click()

            # Allow time for new fighters to load after clicking
            time.sleep(2)
        
        except:
            # If the button is no longer found, break the loop
            print("All fighters loaded or button not found.")
            break

# Call the function to load all fighters
load_all_fighters()

# Locate all fighter entries
fighters = driver.find_elements(By.CLASS_NAME, "fighter-item-wrapper")

# Create a directory to store the images
if not os.path.exists("fighter_images"):
    os.makedirs("fighter_images")

# Function to save images with proper names
def save_fighter_image(fighter):
    try:
        # Find the fighter name
        name_element = fighter.find_element(By.CLASS_NAME, "fighter-labels").find_element(By.TAG_NAME, "h4")
        fighter_name = name_element.text.strip().title()
        
        # Remove accents and non-ASCII characters
        fighter_name = unidecode(fighter_name)  # Transliterate to ASCII
        fighter_name = re.sub(r'[^a-zA-Z0-9 ]', '', fighter_name)  # Remove non-alphanumeric characters
        
        # Replace spaces with underscores
        fighter_name = fighter_name.replace(" ", "_")

        # Find the fighter image
        image_element = fighter.find_element(By.TAG_NAME, "img")
        image_url = image_element.get_attribute("src")

        # Create a custom request header to mimic a browser
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

        # Create the request with the custom headers
        req = urllib.request.Request(image_url, headers=headers)

        # Open the URL and save the image
        image_path = f"fighter_images/{fighter_name}.jpg"
        with urllib.request.urlopen(req) as response, open(image_path, 'wb') as out_file:
            out_file.write(response.read())

        print(f"Downloaded {fighter_name}.jpg")

    except Exception as e:
        print(f"Error downloading image for fighter {fighter_name}: {e}")

# Iterate through all fighters and save their images
for fighter in fighters:
    save_fighter_image(fighter)

# Close the driver
driver.quit()
