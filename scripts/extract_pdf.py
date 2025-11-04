from pypdf import PdfReader
p = '/workspaces/tf-redes/Definição_TF_20252-Turma 30.pdf'
reader = PdfReader(p)
text = []
for i,page in enumerate(reader.pages,1):
    t = page.extract_text()
    text.append(f"--- PAGE {i} ---\n" + (t or "[no text extracted]"))
print('\n\n'.join(text))
