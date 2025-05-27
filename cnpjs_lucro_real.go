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

	"github.com/xuri/excelize/v2"
)

const (
	ARQUIVO_ENTRADA = `C:\Users\rfsra\OneDrive\Desktop\Lucro Real Camila\cnpj_limpos_unido_orign_pos_cnae.csv`
	ARQUIVO_SAIDA   = `C:\Users\rfsra\OneDrive\Desktop\Lucro Real Camila\resultado_consulta_cnae_golang_I.xlsx`
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

func salvarXLSX(dados []map[string]string, path string) error {
	f := excelize.NewFile()
	defer f.Close()

	// Cria uma nova planilha
	index, err := f.NewSheet("Sheet1")
	if err != nil {
		return err
	}

	// Define o cabeçalho
	headers := []string{"CNPJ", "Nome", "UF", "Cidade", "E-mail", "Link", "CNAE Código", "CNAE Descrição", "Data/Hora"}
	for i, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue("Sheet1", cell, header)
	}

	// Preenche os dados
	for rowIdx, rowData := range dados {
		row := rowIdx + 2 // +1 para o cabeçalho, +1 porque começa em 1
		values := []string{
			rowData["cnpj"],
			rowData["nome"],
			rowData["uf"],
			rowData["cidade"],
			rowData["email"],
			rowData["link"],
			rowData["cnae_codigo"],
			rowData["cnae_desc"],
			rowData["data_hora"],
		}

		for colIdx, value := range values {
			cell, _ := excelize.CoordinatesToCellName(colIdx+1, row)
			f.SetCellValue("Sheet1", cell, value)
		}
	}

	// Define a planilha ativa
	f.SetActiveSheet(index)

	// Salva o arquivo
	if err := f.SaveAs(path); err != nil {
		return err
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
	total := len(cnpjs)

	// Adiciona um espaço no final para limpar qualquer caractere residual
	defer fmt.Printf("\nConsulta finalizada! Total processado: %d\n", total)

	var resultados []map[string]string
	for i, cnpj := range novosCNPJs {
		current := startIndex + i + 1
		fmt.Printf("\rProcessando [%d/%d] - CNPJ: %s", current, total, formatarCNPJ(cnpj))

		dados, err := consultarCNPJ(cnpj)
		if err != nil {
			fmt.Println("\nErro na consulta:", err)
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
			fmt.Println("\nErro ao salvar checkpoint:", err)
		}

		// Salva a cada 10 registros ou no último
		if (i+1)%10 == 0 || i == len(novosCNPJs)-1 {
			err = salvarXLSX(resultados, ARQUIVO_SAIDA)
			if err != nil {
				fmt.Println("\nErro ao salvar XLSX:", err)
			}
			resultados = []map[string]string{}
		}

		time.Sleep(PAUSA_SEGUNDOS * time.Second)
	}
}
