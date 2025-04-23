Para executar o sistema:

1. Clonar o repositório
2. Criar o ambiente virtual:

```console
python -m venv venv
```

3. Ativar o ambiente virtual (no windows o arquivo é o .bat):

```console
.venv/Scripts/activate.bat
```

4. Instalar as dependências:

```console
pip install -r requirements.txt
```

5. Executar a aplicação

```console
uvicorn main:app --reload --port 8001
```
