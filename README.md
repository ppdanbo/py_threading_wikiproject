# py_threading_wikiproject  

An experimental and learning project to practice **Python concurrency** by combining **threading** and **queuing** while scraping and parsing content from **Wikipedia**.  

---

## 📌 Features  
- Uses Python’s built-in **threading** module for concurrency  
- Implements **queue.Queue** to manage work distribution across threads  
- Scrapes Wikipedia pages using `requests`  
- Parses HTML content with **BeautifulSoup4**  
- Compares sequential vs. threaded execution for learning purposes  

---

## 🚀 Getting Started  
### Prerequisites  
- Python **3.12**  

Install dependencies:  
```bash

pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pip install --only-binary=:all: yarl
```

## 🚀 Installation & Run

### Clone the repository
```bash
git clone https://github.com/your-username/py_threading_wikiproject.git
cd py_threading_wikiproject
```

### Run the script
```bash
cd py_threading_wikiproject
python main.py
```

## Technologies

Python 3

- threading – concurrency with threads
- queue.Queue – task management between producer/consumer threads
- Apply web scraping and parsing with BeautifulSoup

### 🎯 Purpose

This repository is for practicing and educational purposes. It was created to:

- Learn and practice threading in Python

- Use queues to coordinate tasks across multiple threads

- Apply web scraping and parsing with BeautifulSoup

### 🤝 Contributing

Ideas to extend this project:

- Add retry logic and error handling

- Save parsed results into a structured format ( JSON, database)

- Benchmark performance: sequential vs. threading + queues
  
