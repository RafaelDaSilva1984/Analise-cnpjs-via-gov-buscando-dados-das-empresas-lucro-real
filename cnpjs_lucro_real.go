package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	ARQUIVO_ENTRADA = `C:\Users\rfsra\OneDrive\Desktop\Lucro Real Camila\cnpj_limpos_unido_orign_pos_cnae.csv`
	ARQUIVO_SAIDA   = `C:\Users\rfsra\OneDrive\Desktop\Lucro Real Camila\resultado_consulta_cnae_golang_I.csv`
	CHECKPOINT_FILE = "checkpoint.txt"
	PAUSA_SEGUNDOS  = 21
	MAX_TENTATIVAS  = 3
)

type Estabelecimento struct {
	Email              string `json:"email"`
	EstadoSigla        string `json:"estado"`
	CidadeNome         string `json:"cidade"`
	AtividadePrincipal struct {
		Subclasse string `json:"subclasse"`
		Descricao string `json:"descricao"`
	} `json:"atividade_principal"`
}

type RespostaAPI struct {
	RazaoSocial     string `json:"razao_social"`
	Estabelecimento struct {
		Email  string `json:"email"`
		Estado struct {
			Sigla string `json:"sigla"`
		} `json:"estado"`
		Cidade struct {
			Nome string `json:"nome"`
		} `json:"cidade"`
		AtividadePrincipal struct {
			Subclasse string `json:"subclasse"`
			Descricao string `json:"descricao"`
		} `json:"atividade_principal"`
	} `json:"estabelecimento"`
}

func limparCNPJ(cnpj string) string {
	cnpj = strings.TrimSpace(cnpj)
	cnpj = strings.ReplaceAll(cnpj, ".", "")
	cnpj = strings.ReplaceAll(cnpj, "-", "")
	cnpj = strings.ReplaceAll(cnpj, "/", "")
	for len(cnpj) < 14 {
		cnpj = "0" + cnpj
	}
	return cnpj
}

func formatarCNPJ(cnpj string) string {
	cnpj = limparCNPJ(cnpj)
	return fmt.Sprintf("%s.%s.%s/%s-%s", cnpj[:2], cnpj[2:5], cnpj[5:8], cnpj[8:12], cnpj[12:])
}

func lerCheckpoint() (string, error) {
	data, err := os.ReadFile(CHECKPOINT_FILE)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func salvarCheckpoint(cnpj string) error {
	return os.WriteFile(CHECKPOINT_FILE, []byte(cnpj), 0644)
}

func consultarCNPJ(cnpj string) (map[string]string, error) {
	url := fmt.Sprintf("https://publica.cnpj.ws/cnpj/%s", cnpj)
	client := &http.Client{Timeout: 35 * time.Second}

	for tentativa := 0; tentativa < MAX_TENTATIVAS; tentativa++ {
		resp, err := client.Get(url)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 404 {
			return map[string]string{"status": "404 - Não encontrado"}, nil
		}

		if resp.StatusCode == 429 {
			time.Sleep(25 * time.Second)
			continue
		}

		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("status inesperado: %d", resp.StatusCode)
		}

		var data RespostaAPI
		err = json.NewDecoder(resp.Body).Decode(&data)
		if err != nil {
			return nil, err
		}

		email := data.Estabelecimento.Email
		if email == "" || email == "N/D" || email == "Erro" || email == "Timeout" {
			email = "e-mail não cadastrado"
		}

		result := map[string]string{
			"cnpj":        formatarCNPJ(cnpj),
			"nome":        data.RazaoSocial,
			"uf":          data.Estabelecimento.Estado.Sigla,
			"cidade":      data.Estabelecimento.Cidade.Nome,
			"email":       email,
			"link":        fmt.Sprintf("https://cnpj.biz/%s", cnpj),
			"cnae_codigo": data.Estabelecimento.AtividadePrincipal.Subclasse,
			"cnae_desc":   data.Estabelecimento.AtividadePrincipal.Descricao,
			"data_hora":   time.Now().Format("2006-01-02 15:04:05"),
		}
		return result, nil
	}

	return map[string]string{
		"cnpj":        formatarCNPJ(cnpj),
		"nome":        "Falha",
		"uf":          "Timeout",
		"cidade":      "Timeout",
		"email":       "e-mail não cadastrado",
		"link":        fmt.Sprintf("https://cnpj.biz/%s", cnpj),
		"cnae_codigo": "Timeout",
		"cnae_desc":   "Timeout",
		"data_hora":   time.Now().Format("2006-01-02 15:04:05"),
	}, nil
}

func lerCSV(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := csv.NewReader(file)
	var cnpjs []string

	// Ignora cabeçalho, se existir
	_, err = r.Read()
	if err != nil {
		return nil, err
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(record) < 2 {
			continue
		}
		cnpjs = append(cnpjs, limparCNPJ(record[1]))
	}
	return cnpjs, nil
}

func salvarCSV(dados []map[string]string, path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Escreve cabeçalho se o arquivo estiver vazio
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		header := []string{"cnpj", "nome", "uf", "cidade", "email", "link", "cnae_codigo", "cnae_desc", "data_hora"}
		writer.Write(header)
	}

	for _, d := range dados {
		row := []string{
			d["cnpj"], d["nome"], d["uf"], d["cidade"], d["email"],
			d["link"], d["cnae_codigo"], d["cnae_desc"], d["data_hora"],
		}
		writer.Write(row)
	}
	return nil
}

func main() {
	cnpjs, err := lerCSV(ARQUIVO_ENTRADA)
	if err != nil {
		panic(err)
	}

	var ultimoCheckpoint string
	ultimoCheckpoint, err = lerCheckpoint()
	if err != nil {
		ultimoCheckpoint = ""
	}

	var startIndex int
	if ultimoCheckpoint != "" {
		for i, c := range cnpjs {
			if c == ultimoCheckpoint {
				startIndex = i + 1
				break
			}
		}
	}

	novosCNPJs := cnpjs[startIndex:]

	var resultados []map[string]string
	for i, cnpj := range novosCNPJs {
		fmt.Printf("Consultando %s (%d/%d)\n", formatarCNPJ(cnpj), startIndex+i+1, len(cnpjs))
		dados, err := consultarCNPJ(cnpj)
		if err != nil {
			fmt.Println("Erro na consulta:", err)
			dados = map[string]string{
				"cnpj":        formatarCNPJ(cnpj),
				"nome":        "Erro",
				"uf":          "Erro",
				"cidade":      "Erro",
				"email":       "e-mail não cadastrado",
				"link":        fmt.Sprintf("https://cnpj.biz/%s", cnpj),
				"cnae_codigo": "Erro",
				"cnae_desc":   "Erro",
				"data_hora":   time.Now().Format("2006-01-02 15:04:05"),
			}
		}
		resultados = append(resultados, dados)

		err = salvarCheckpoint(cnpj)
		if err != nil {
			fmt.Println("Erro ao salvar checkpoint:", err)
		}

		// Salva a cada 10 registros ou no último
		if (i+1)%10 == 0 || i == len(novosCNPJs)-1 {
			err = salvarCSV(resultados, ARQUIVO_SAIDA)
			if err != nil {
				fmt.Println("Erro ao salvar CSV:", err)
			}
			resultados = []map[string]string{}
		}

		time.Sleep(PAUSA_SEGUNDOS * time.Second)
	}

	fmt.Println("Consulta finalizada!")
}
