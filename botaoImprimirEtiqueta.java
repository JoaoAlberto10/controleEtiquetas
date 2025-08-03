import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.modelcore.PlatformService;
import br.com.sankhya.modelcore.PlatformServiceFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class botaoImprimirEtiqueta implements AcaoRotinaJava{
    @Override
    public void doAction(ContextoAcao ctx) throws Exception {
        Registro[] linhasSelecionadas = ctx.getLinhas();
        PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");

		/*
		 * Se Quiser Usar Passando Parametros, Utilize o Proprio Sistema chamando ctx.getParam(arg0), atribuia a uma variavel, e altere nas Chamadas do Plataform.
		 *
		 * 	Service: @core:report.service
		   	Description: Serviço para impressão de relatórios
			Arguments: [4]

			Name: codemp
			Type: BigDecimal
			Description: Cód. da empresa
			Required: true

			Name: nurfe
			Type: BigDecimal
			Description: Nro.único do relatório
			Required: true

			Name: report.params
			Type: Map<String, Object>
			Description: Parâmetros para execução do relatório
			Required: true

			Name: printer.name
			Type: String
			Description: Nome local da impressora
			Required: true
		 *
		 */

        String p_nuNota = (String) ctx.getParam("P_NUNOTA");

        //int paramImpressao = Integer.valueOf(tipoImpressao);

        //reportService.set("nurfe", BigDecimal.valueOf(paramImpressao));

	/*
	Estou assumindo um nome padrão para impressora.
	Se for usar o SPS então deverá haver uma impressora registrada com esse nome ou existir uma roteirização deste nome para o nome real da impressora.
	Caso não use o SPS, então o sistema vai perguntar qual impressora deve ser usada (isso pode ser salvo para não perguntar novamente)
	 */
        reportService.set("printer.name", "ZD230CPO01");
        reportService.set("codemp", BigDecimal.valueOf(1) );  //a empresa é importante para que o roteamento seja feito corretamente, caso exista.



            //if (paramImpressao == 86 ) {

                Map<String, Object> parameters = new HashMap<String, Object>();

                //parameters.put("P_NUNOTA", linha.getCampo("CODPROD"));
                parameters.put("P_QUANT_ETIQ", ctx.getParam("P_QUANT_ETIQ"));
                parameters.put("P_CODEMP2", ctx.getParam("P_CODEMP2"));

                reportService.set("report.params", parameters);
                System.out.println("Executando o relatório - ASM_SNK");
                reportService.execute();





    }
}