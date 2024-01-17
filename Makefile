install:
	pip install -r ./requirements.txt

run:
	uvicorn app.main:app --reload