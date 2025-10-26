import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.PersistenceException;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.PlatformService;
import br.com.sankhya.modelcore.PlatformServiceFactory;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class TRG_CONFERENCIA_TGFITE implements EventoProgramavelJava {

    // =============================== Helpers TOP ===============================

    private boolean isTopConferencia(BigDecimal top){
        if (top == null) return false;
        int t = top.intValue();
        return (t == 1701 || t == 1702 || t == 1703 || t == 1704);
    }

    // Retorna true se o handler deve ser ignorado por não ser TOP de conferência
    private boolean ignorarSeNaoConferencia(PersistenceEvent event) throws Exception {
        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();
        BigDecimal top = obterTopDaNota(jdbc, vo.asBigDecimal("NUNOTA"));
        return !isTopConferencia(top);
    }

    // =============================== Ciclo de Vida ===============================

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        if (ignorarSeNaoConferencia(event)) return;

        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();

        BigDecimal top = obterTopDaNota(jdbc, vo.asBigDecimal("NUNOTA"));
        String codBarra = vo.asString("CODBARRAPDV");

        if (codBarra == null || codBarra.trim().isEmpty()) {
            // Inclusão manual (sem etiqueta)
            // Apenas valida que é conferência e deixa inserir normalmente
            return;
        }

        boolean isPalete = existePaleteAtivoPorCodigo(jdbc, codBarra); // ATIVO='S'

        if (top.intValue() == 1704) { // Inventário
            if (isPalete) {
                validarInventarioPalete(vo, event);
            } else {
                validarConferencia(vo, event, top); // duplicidade na própria nota
            }
            return;
        }

        // 1701/1702/1703
        if (isPalete) {
            validarConferenciaPalete(vo, event, top);
            processarConferenciaPalete(vo, jdbc, top);
        } else {
            validarConferencia(vo, event, top);
            gerarEtiquetaPalete(vo, jdbc, top);
        }

    }

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {
        if (ignorarSeNaoConferencia(event)) return;
        // sem lógica adicional
    }

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {
        if (ignorarSeNaoConferencia(event)) return;
        // sem lógica adicional
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        if (ignorarSeNaoConferencia(event)) return;
        // sem lógica adicional
    }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {
        if (ignorarSeNaoConferencia(event)) return;
    }

    @Override
    public void afterDelete(PersistenceEvent event) throws  Exception{
        try {
            if (ignorarSeNaoConferencia(event)) return;

            DynamicVO vo = (DynamicVO) event.getVo();
            JdbcWrapper jdbc = event.getJdbcWrapper();

            BigDecimal top = obterTopDaNota(jdbc, vo.asBigDecimal("NUNOTA"));
            if (top.intValue() == 1701 || top.intValue() == 1702 || top.intValue() == 1703) return; // só reverte na 1701

            String codBarra = vo.asString("CODBARRAPDV");
            if (existePaleteQualquerPorCodigo(jdbc, codBarra)) {
                // linha excluída era palete
                desfazerConferenciaDoPaleteSaida(jdbc, codBarra);
            } else {
                // linha excluída era etiqueta de item
                desfazerConferenciaDoItemSaida_InativandoPalete(jdbc, codBarra);
            }
        } catch (Exception e) {
            throw new MGEModelException("Item excluído, porém ocorreu um erro ao reverter etiqueta/palete: " + e.getMessage());
        }
    }

    @Override public void beforeCommit(TransactionContext tranCtx) { }

    // =============================== Validações (Item) ===============================

    private void validarConferencia(DynamicVO notaVO, PersistenceEvent event, BigDecimal top) throws Exception {
        JdbcWrapper jdbc = event.getJdbcWrapper();
        BigDecimal nuNota = notaVO.asBigDecimal("NUNOTA");
        String codBarra = notaVO.asString("CODBARRAPDV");
        String codVolNovo = notaVO.asString("CODVOL");
        BigDecimal codProd = notaVO.asBigDecimal("CODPROD");
        BigDecimal qtdconf = BigDecimal.ONE;
        BigDecimal codLocalItem = notaVO.asBigDecimal("CODLOCALORIG");

        // Bloqueia bipar palete + suas caixas na mesma nota (cheque bidirecional pelo lado do item)
        NativeSql sqlVerifica = new NativeSql(jdbc);
        sqlVerifica.appendSql(
                "SELECT 1 " +
                        "  FROM AD_FTICODBARRAITEM ETI " +
                        " WHERE ( (ETI.CODBARRA = :CODBARRA AND EXISTS (SELECT 1 FROM TGFITE I2 WHERE I2.NUNOTA = :NUNOTA AND I2.CODBARRAPDV = ETI.CODBARRAPALETE)) " +
                        "     OR (ETI.CODBARRAPALETE = :CODBARRA AND EXISTS (SELECT 1 FROM TGFITE I3 WHERE I3.NUNOTA = :NUNOTA AND I3.CODBARRAPDV = ETI.CODBARRA)) )"
        );
        sqlVerifica.setNamedParameter("CODBARRA", codBarra);
        sqlVerifica.setNamedParameter("NUNOTA", nuNota);
        try (ResultSet rs = sqlVerifica.executeQuery()) {
            if (rs.next()) {
                throw new MGEModelException("Não é permitido bipar o palete e suas caixas na mesma nota!");
            }
        }

        if (top.intValue() == 1704) {
            // Inventário: só duplicidade na própria nota
            NativeSql sqlDup = new NativeSql(jdbc);
            sqlDup.appendSql("SELECT COUNT(*) QT FROM TGFITE WHERE NUNOTA = :NUNOTA AND CODBARRAPDV = :CODB");
            sqlDup.setNamedParameter("NUNOTA", nuNota);
            sqlDup.setNamedParameter("CODB", codBarra);
            try (ResultSet rs = sqlDup.executeQuery()) {
                if (rs.next() && rs.getBigDecimal("QT").compareTo(BigDecimal.ZERO) > 0)
                    throw new MGEModelException("Este código de barras já foi informado nesta nota de inventário!");
            }
            return;
        }

        // Nota origem e local da etiqueta
        BigDecimal nuNotaOrig = obterNuNotaOrig(jdbc, nuNota);

        BigDecimal topOrigem = obterTopDaNota(jdbc, nuNotaOrig);
        /*if (topOrigem != null && topOrigem.intValue() == 406) {
            NativeSql validacao = new NativeSql(jdbc);
            validacao.appendSql(
                    "SELECT 1 FROM AD_FTICODBARRAITEM " +
                            " WHERE CODBARRA = :CB AND NUNOTA = :NUNOTA_ORIG"
            );
            validacao.setNamedParameter("CB", codBarra);
            validacao.setNamedParameter("NUNOTA_ORIG", nuNotaOrig);

            try (ResultSet rs = validacao.executeQuery()) {
                if (!rs.next()) {
                    throw new MGEModelException(
                            "Etiqueta " + codBarra + " não pertence ao pedido original número " + nuNotaOrig +
                                    ". Apenas etiquetas dessa nota podem ser conferidas (TOP 406)."
                    );
                }
            }
        }*/


        BigDecimal codLocalEtiqueta = null;
        NativeSql sqlLoc = new NativeSql(jdbc);
        sqlLoc.appendSql(
                "SELECT CODLOCAL FROM AD_FTICODBARRAITEM WHERE CODBARRA = :CB AND ATIVO='S' " +
                        "UNION ALL " +
                        "SELECT CODLOCAL FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB AND ATIVO='S'"
        );
        sqlLoc.setNamedParameter("CB", codBarra);
        try (ResultSet rs = sqlLoc.executeQuery()) {
            if (rs.next()) codLocalEtiqueta = rs.getBigDecimal(1);
            else throw new MGEModelException("Código de barras inválido ou inativo.");
        }

        // Valida local por TOP
        if (top.intValue() == 1701) {
            if (codLocalEtiqueta == null || codLocalItem == null || !codLocalEtiqueta.equals(codLocalItem))
                throw new MGEModelException("Local da etiqueta não corresponde ao local informado na nota (Saída).");
        } else if (top.intValue() == 1702) {
            if (codLocalEtiqueta != null)
                throw new MGEModelException("Etiqueta já possui local preenchido, deveria estar NULL (Entrada).");
        } else if (top.intValue() == 1703) {
            // ✅ valida apenas linhas com SEQUENCIA > 0
            BigDecimal sequencia = notaVO.asBigDecimal("SEQUENCIA");
            if (sequencia == null || sequencia.compareTo(BigDecimal.ZERO) <= 0) {
                // Linha "negativa" ou de retorno → não valida
                return;
            }

            // Busca origem/destino da conferência
            BigDecimal origem = null;
            BigDecimal destino = null;

            NativeSql s = new NativeSql(jdbc);
            s.appendSql("SELECT CODLOCAL, CODLOCALDEST FROM AD_FTICONFERENCIA WHERE NUNOTACONF = :N");
            s.setNamedParameter("N", nuNota);
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    origem = rs.getBigDecimal("CODLOCAL");
                    destino = rs.getBigDecimal("CODLOCALDEST");
                }
            }

            BigDecimal codLocalLinha = notaVO.asBigDecimal("CODLOCALORIG");

            if (origem != null && destino != null && codLocalLinha != null) {
                if (codLocalLinha.compareTo(origem) == 0) {
                    // Linha de ORIGEM → etiqueta deve estar no local de origem
                    if (codLocalEtiqueta == null || codLocalEtiqueta.compareTo(origem) != 0)
                        throw new MGEModelException("Etiqueta deve estar no local de ORIGEM (Transferência).");
                } else if (codLocalLinha.compareTo(destino) == 0) {
                    // Linha de DESTINO → aceitar etiqueta ainda no ORIGEM ou já no DESTINO
                    if (codLocalEtiqueta == null ||
                            !(codLocalEtiqueta.compareTo(origem) == 0 || codLocalEtiqueta.compareTo(destino) == 0))
                        throw new MGEModelException("Etiqueta não corresponde nem à origem nem ao destino da transferência.");
                }
            }
        }


        // Quantidade pendente e checks
        BigDecimal[] pend = calcularPendencias(jdbc, nuNota, nuNotaOrig, codProd, codVolNovo, codBarra);
        BigDecimal codBarraConferido = pend[0];
        BigDecimal codVolPedido     = pend[1];
        BigDecimal qtdPendente      = pend[2] == null ? BigDecimal.ZERO : pend[2];

        if (qtdconf.compareTo(qtdPendente) > 0)
            throw new MGEModelException("Quantidade acima do pedido original!");

        if (codVolPedido == null || codVolPedido.compareTo(BigDecimal.ZERO) == 0)
            throw new MGEModelException("Volume informado não faz parte do pedido original!");

        if (codBarraConferido != null && codBarraConferido.compareTo(BigDecimal.ZERO) > 0)
            throw new MGEModelException("Este código de barras já foi conferido anteriormente!");
    }

    // =============================== Validações (Palete) ===============================

    private void validarConferenciaPalete(DynamicVO vo, PersistenceEvent event, BigDecimal top) throws Exception {
        JdbcWrapper jdbc = event.getJdbcWrapper();
        BigDecimal nuNota = vo.asBigDecimal("NUNOTA");
        String codBarraPalete = vo.asString("CODBARRAPDV");
        BigDecimal codProd = vo.asBigDecimal("CODPROD");
        String codVolNovo = vo.asString("CODVOL");
        BigDecimal codLocalItem = vo.asBigDecimal("CODLOCALORIG");

        // Proibir bipar palete se já existem caixas dele conferidas na mesma nota
        NativeSql mix = new NativeSql(jdbc);
        mix.appendSql(
                "SELECT 1 " +
                        "  FROM TGFITE I " +
                        "  JOIN AD_FTICODBARRAITEM ETI ON ETI.CODBARRA = I.CODBARRAPDV " +
                        " WHERE I.NUNOTA = :NUNOTA AND ETI.CODBARRAPALETE = :PAL"
        );
        mix.setNamedParameter("NUNOTA", nuNota);
        mix.setNamedParameter("PAL", codBarraPalete);
        try (ResultSet rs = mix.executeQuery()) {
            if (rs.next())
                throw new MGEModelException("Não é permitido bipar o palete se já existem caixas desse palete conferidas nesta nota!");
        }

        BigDecimal codLocalDoPalete = obterLocalDoPalete(jdbc, codBarraPalete);

        if (top.intValue() == 1701) {
            if (codLocalDoPalete == null || codLocalItem == null || codLocalDoPalete.compareTo(codLocalItem) != 0)
                throw new MGEModelException("Local do palete não corresponde ao local informado na nota (Saída).");
        } else if (top.intValue() == 1702) {
            if (codLocalDoPalete != null)
                throw new MGEModelException("Palete já possui local preenchido, deveria estar NULL (Entrada).");
        } else if (top.intValue() == 1703) {
            BigDecimal codLocalConf = obterLocalDestino(jdbc, nuNota);
            if (codLocalDoPalete == null || codLocalConf == null || codLocalDoPalete.compareTo(codLocalConf) != 0)
                throw new MGEModelException("Local do palete não corresponde ao local da conferência (Transferência).");
        }

        // Pendente: palete vale a QTD do palete
        BigDecimal nuNotaOrig = obterNuNotaOrig(jdbc, nuNota);
        BigDecimal qtdconf = obterQtdPalete(jdbc, codBarraPalete);
        if (qtdconf == null) qtdconf = BigDecimal.ZERO;

        BigDecimal qtdPendente = calcularQtdPendente(jdbc, nuNota, nuNotaOrig, codProd, codVolNovo);
        if (qtdPendente == null) qtdPendente = BigDecimal.ZERO;

        if (qtdconf.compareTo(qtdPendente) > 0)
            throw new MGEModelException("Quantidade do palete acima do pendente do pedido!");

        // Reforço: volume do palete deve existir no pedido original
        BigDecimal existeVol = checarVolNoPedidoOriginal(jdbc, nuNotaOrig, codProd, codVolNovo);
        if (existeVol == null || existeVol.compareTo(BigDecimal.ZERO) == 0)
            throw new MGEModelException("Volume do palete não faz parte do pedido original!");
    }

    private void validarInventarioPalete(DynamicVO vo, PersistenceEvent event) throws Exception {
        JdbcWrapper jdbc = event.getJdbcWrapper();
        BigDecimal nuNota = vo.asBigDecimal("NUNOTA");
        String codBarraPalete = vo.asString("CODBARRAPDV");

        NativeSql sqlDup = new NativeSql(jdbc);
        sqlDup.appendSql("SELECT COUNT(*) QT FROM TGFITE WHERE NUNOTA = :NUNOTA AND CODBARRAPDV = :CB");
        sqlDup.setNamedParameter("NUNOTA", nuNota);
        sqlDup.setNamedParameter("CB", codBarraPalete);

        try (ResultSet rs = sqlDup.executeQuery()) {
            if (rs.next() && rs.getBigDecimal("QT").compareTo(BigDecimal.ZERO) > 0)
                throw new MGEModelException("Este palete já foi informado nesta nota de inventário!");
        }
    }

    // =============================== Processamentos (Palete / Item) ===============================

    private void processarConferenciaPalete(DynamicVO vo, JdbcWrapper jdbc, BigDecimal top) throws Exception {
        final BigDecimal nuNota = vo.asBigDecimal("NUNOTA");
        final BigDecimal codLocalItem = vo.asBigDecimal("CODLOCALORIG");
        final String codBarraPalete = vo.asString("CODBARRAPDV");

        BigDecimal local = (top.intValue() == 1703) ? obterLocalDestino(jdbc, nuNota) : codLocalItem;

        if (top.intValue() == 1701) {
            // Saída: inativa palete e TODAS as caixas filhas
            NativeSql upPal = new NativeSql(jdbc);
            upPal.appendSql("UPDATE AD_FTICODBARRAPALETE SET ATIVO='N', CODLOCAL = :LOCAL WHERE CODBARRAPALETE = :PAL AND ATIVO='S'");
            upPal.setNamedParameter("LOCAL", local);
            upPal.setNamedParameter("PAL", codBarraPalete);
            upPal.executeUpdate();

            NativeSql upItens = new NativeSql(jdbc);
            upItens.appendSql("UPDATE AD_FTICODBARRAITEM SET ATIVO='N', CODLOCAL = :LOCAL WHERE CODBARRAPALETE = :PAL AND ATIVO='S'");
            upItens.setNamedParameter("LOCAL", local);
            upItens.setNamedParameter("PAL", codBarraPalete);
            upItens.executeUpdate();

        } else if (top.intValue() == 1702) {
            // Entrada: define local do palete
            NativeSql upPal = new NativeSql(jdbc);
            upPal.appendSql("UPDATE AD_FTICODBARRAPALETE SET CODLOCAL = :LOCAL WHERE CODBARRAPALETE = :PAL AND ATIVO='S'");
            upPal.setNamedParameter("LOCAL", local);
            upPal.setNamedParameter("PAL", codBarraPalete);
            upPal.executeUpdate();

        } else if (top.intValue() == 1703) {
            // Transferência: mover palete E TODAS as caixas para o local da conferência
            NativeSql valida = new NativeSql(jdbc);
            valida.appendSql(
                    "SELECT COUNT(DISTINCT NVL(CODLOCAL,-1)) QTD " +
                            "  FROM AD_FTICODBARRAITEM " +
                            " WHERE CODBARRAPALETE = :PAL AND ATIVO='S' AND SEQUENCIA>0"
            );
            valida.setNamedParameter("PAL", codBarraPalete);
            int distintos = 0;
            try (ResultSet rs = valida.executeQuery()) {
                if (rs.next()) distintos = rs.getInt("QTD");
            }
            if (distintos > 1) {
                throw new MGEModelException("Não é possível transferir: há itens do palete com locais diferentes. Normalize antes.");
            }

            NativeSql upPal = new NativeSql(jdbc);
            upPal.appendSql("UPDATE AD_FTICODBARRAPALETE SET CODLOCAL = :LOCAL WHERE CODBARRAPALETE = :PAL AND ATIVO='S'");
            upPal.setNamedParameter("LOCAL", local);
            upPal.setNamedParameter("PAL", codBarraPalete);
            upPal.executeUpdate();

            NativeSql upItens = new NativeSql(jdbc);
            upItens.appendSql("UPDATE AD_FTICODBARRAITEM SET CODLOCAL = :LOCAL WHERE CODBARRAPALETE = :PAL AND ATIVO='S'");
            upItens.setNamedParameter("LOCAL", local);
            upItens.setNamedParameter("PAL", codBarraPalete);
            upItens.executeUpdate();
        }
        // 1704: nada a fazer (apenas valida duplicidade em validarInventarioPalete)
    }

    private void gerarEtiquetaPalete(DynamicVO vo, JdbcWrapper jdbc, BigDecimal top) throws Exception {
        final BigDecimal nuNota = vo.asBigDecimal("NUNOTA");
        final BigDecimal codProd = vo.asBigDecimal("CODPROD");
        final BigDecimal seqVol = vo.asBigDecimal("AD_SEQVOL");
        final BigDecimal codUsu = vo.asBigDecimal("AD_CODUSUINC");
        final String codVol = vo.asString("CODVOL");
        final BigDecimal codLocalOrig = vo.asBigDecimal("CODLOCALORIG");
        final String codBarraItem = vo.asString("CODBARRAPDV");

        BigDecimal local = (top.intValue() == 1703) ? obterLocalDestino(jdbc, nuNota) : codLocalOrig;

        // Atualiza CODLOCAL do item bipado; na 1701 também inativa etiqueta
        NativeSql upd2 = new NativeSql(jdbc);
        upd2.appendSql("UPDATE AD_FTICODBARRAITEM SET CODLOCAL = :LOCAL" +
                (top.intValue() == 1701 ? ", ATIVO='N'" : "") +
                " WHERE CODBARRA = :ETI AND ATIVO='S'");
        upd2.setNamedParameter("LOCAL", local);
        upd2.setNamedParameter("ETI", codBarraItem);
        upd2.executeUpdate();

        // Se item pertencia a palete, desmonta palete (inativa palete existente)
        NativeSql sqlPalete = new NativeSql(jdbc);
        sqlPalete.appendSql("SELECT CODBARRAPALETE FROM AD_FTICODBARRAITEM WHERE CODBARRA = :ETI AND CODBARRAPALETE IS NOT NULL");
        sqlPalete.setNamedParameter("ETI", codBarraItem);
        try (ResultSet rs = sqlPalete.executeQuery()) {
            if (rs.next()) {
                String codPalete = rs.getString("CODBARRAPALETE");
                NativeSql desativa = new NativeSql(jdbc);
                desativa.appendSql("UPDATE AD_FTICODBARRAPALETE SET ATIVO='N' WHERE CODBARRAPALETE = :PAL");
                desativa.setNamedParameter("PAL", codPalete);
                desativa.executeUpdate();
            }
        }

        if (seqVol == null) return;

        Integer itensPorPalete = obterQtdPorPalete(jdbc, codProd, codVol);
        if (itensPorPalete == null || itensPorPalete <= 0) return;

        int totalJaGravados = contarItensIvc(jdbc, nuNota, codProd, codVol, seqVol) + 1;

        String tipoPalete = codVol;
        String seqPalete3 = String.format("%03d", seqVol.intValue());
        String codBarraPalete = codProd.toPlainString() + tipoPalete + "-" + nuNota.toPlainString() + "-" + seqPalete3;

        // Vincula item ao palete (se ainda não estiver)
        NativeSql upd = new NativeSql(jdbc);
        upd.appendSql("UPDATE AD_FTICODBARRAITEM SET CODBARRAPALETE = :PAL WHERE CODBARRA = :ETI AND CODBARRAPALETE IS NULL");
        upd.setNamedParameter("PAL", codBarraPalete);
        upd.setNamedParameter("ETI", codBarraItem);
        upd.executeUpdate();

        // Se completou o palete, cria registro do palete e imprime
        if (itensPorPalete == totalJaGravados) {
            NativeSql ins = new NativeSql(jdbc);
            ins.appendSql(
                    "INSERT INTO AD_FTICODBARRAPALETE (NROCONFIG, CODBARRAPALETE, CODPROD, CODVOL, QTD, DHINC, CODUSU, CODLOCAL, NUNOTA, ATIVO) " +
                            "VALUES (1, :CODBARRA, :CODPROD, :CODVOL, :QTD, SYSDATE, :CODUSU, :LOCAL, :NUNOTA, 'S')"
            );
            ins.setNamedParameter("CODBARRA", codBarraPalete);
            ins.setNamedParameter("CODPROD", codProd);
            ins.setNamedParameter("CODVOL", tipoPalete);
            ins.setNamedParameter("QTD", itensPorPalete);
            ins.setNamedParameter("CODUSU", codUsu);
            ins.setNamedParameter("LOCAL", local);
            ins.setNamedParameter("NUNOTA", nuNota);
            ins.executeUpdate();

            String localPrinterName = obterImpressora(jdbc, local);
            PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
            reportService.set("printer.name", localPrinterName);
            reportService.set("nurfe", 286);
            reportService.set("codemp", BigDecimal.ONE);

            Map<String, Object> parameters = new HashMap<>();
            parameters.put("P_CODBARRA", codBarraPalete);
            reportService.set("report.params", parameters);
            reportService.execute();
        }
    }

    // =============================== Reversões (1701) ===============================

    private void desfazerConferenciaDoItemSaida_InativandoPalete(JdbcWrapper jdbc, String codBarraItem) throws Exception {
        // Reativar etiqueta
        NativeSql upEtiqueta = new NativeSql(jdbc);
        upEtiqueta.appendSql("UPDATE AD_FTICODBARRAITEM SET ATIVO='S' WHERE CODBARRA = :CB");
        upEtiqueta.setNamedParameter("CB", codBarraItem);
        upEtiqueta.executeUpdate();

        // Descobrir palete vinculado
        String codPalete = null;
        NativeSql selPal = new NativeSql(jdbc);
        selPal.appendSql("SELECT CODBARRAPALETE FROM AD_FTICODBARRAITEM WHERE CODBARRA = :CB");
        selPal.setNamedParameter("CB", codBarraItem);
        try (ResultSet rs = selPal.executeQuery()) {
            if (rs.next()) codPalete = rs.getString(1);
        }

        // Desvincular o item do palete
        NativeSql limpaVinc = new NativeSql(jdbc);
        limpaVinc.appendSql("UPDATE AD_FTICODBARRAITEM SET CODBARRAPALETE = NULL WHERE CODBARRA = :CB");
        limpaVinc.setNamedParameter("CB", codBarraItem);
        limpaVinc.executeUpdate();

        // Regra: tirou UMA etiqueta do palete -> INATIVA o palete imediatamente
        if (codPalete != null && !codPalete.isEmpty()) {
            NativeSql inativaPal = new NativeSql(jdbc);
            inativaPal.appendSql("UPDATE AD_FTICODBARRAPALETE SET ATIVO='N' WHERE CODBARRAPALETE = :PAL");
            inativaPal.setNamedParameter("PAL", codPalete);
            inativaPal.executeUpdate();

            // (opcional) desmontar tudo (limpar vínculo de todos os demais itens):
            // NativeSql desmonta = new NativeSql(jdbc);
            // desmonta.appendSql("UPDATE AD_FTICODBARRAITEM SET CODBARRAPALETE = NULL WHERE CODBARRAPALETE = :PAL");
            // desmonta.setNamedParameter("PAL", codPalete);
            // desmonta.executeUpdate();
        }
    }

    private void desfazerConferenciaDoPaleteSaida(JdbcWrapper jdbc, String codBarraPalete) throws Exception {
        // Reativar palete
        NativeSql upPal = new NativeSql(jdbc);
        upPal.appendSql("UPDATE AD_FTICODBARRAPALETE SET ATIVO='S' WHERE CODBARRAPALETE = :PAL");
        upPal.setNamedParameter("PAL", codBarraPalete);
        upPal.executeUpdate();

        // Reativar todas as caixas do palete
        NativeSql upItens = new NativeSql(jdbc);
        upItens.appendSql("UPDATE AD_FTICODBARRAITEM SET ATIVO='S' WHERE CODBARRAPALETE = :PAL");
        upItens.setNamedParameter("PAL", codBarraPalete);
        upItens.executeUpdate();

        // (opcional) reverter CODLOCAL para NULL (palete e itens)
        // NativeSql clr1 = new NativeSql(jdbc);
        // clr1.appendSql("UPDATE AD_FTICODBARRAPALETE SET CODLOCAL=NULL WHERE CODBARRAPALETE=:PAL");
        // clr1.setNamedParameter("PAL", codBarraPalete);
        // clr1.executeUpdate();
        //
        // NativeSql clr2 = new NativeSql(jdbc);
        // clr2.appendSql("UPDATE AD_FTICODBARRAITEM SET CODLOCAL=NULL WHERE CODBARRAPALETE=:PAL");
        // clr2.setNamedParameter("PAL", codBarraPalete);
        // clr2.executeUpdate();
    }

    // =============================== Helpers de Pendência ===============================

    private BigDecimal[] calcularPendencias(JdbcWrapper jdbc, BigDecimal nuNotaConf, BigDecimal nuNotaOrig,
                                            BigDecimal codProd, String codVolNovo, String codBarra) throws Exception {
        BigDecimal codBarraConferido = null;
        BigDecimal codVolPedido = null;
        BigDecimal qtdPendente = null;

        NativeSql sql2 = new NativeSql(jdbc);
        sql2.appendSql(
                "SELECT " +
                        " (SELECT COUNT(*) FROM TGFITE WHERE NUNOTA = :NUNOTACONF AND CODBARRAPDV = :CODBARRA) AS CODBARRAREPETIDO, " +
                        " (SELECT COUNT(*) FROM TGFITE ITE WHERE ITE.NUNOTA = :NUNOTAORIG AND ITE.CODPROD = :CODPROD AND (ITE.CODVOL = :CODVOL OR ITE.AD_CODVOL = :CODVOL)) AS CODVOLPEDIDO, " +
                        " NVL(( NVL((SELECT SUM(CASE " +
                        "   WHEN ITE.AD_CODVOL IS NULL AND ITE.CODVOL = PRO.CODVOL THEN ITE.QTDNEG " +
                        "   WHEN ITE.AD_CODVOL IS NULL AND ITE.CODVOL <> PRO.CODVOL THEN ITE.QTDNEG / NVL(VOA.QUANTIDADE,1) " +
                        "   WHEN ITE.AD_CODVOL IS NOT NULL AND ITE.AD_CODVOL = PRO.CODVOL THEN ITE.QTDNEG " +
                        "   WHEN ITE.AD_CODVOL IS NOT NULL AND ITE.AD_CODVOL <> PRO.CODVOL THEN ITE.QTDNEG / NVL(VOA2.QUANTIDADE,1) END) " +
                        "   FROM TGFITE ITE JOIN TGFPRO PRO ON PRO.CODPROD = ITE.CODPROD " +
                        "   LEFT JOIN TGFVOA VOA ON VOA.CODPROD = ITE.CODPROD AND VOA.CODVOL = ITE.CODVOL " +
                        "   LEFT JOIN TGFVOA VOA2 ON VOA2.CODPROD = ITE.CODPROD AND VOA2.CODVOL = ITE.AD_CODVOL " +
                        "   WHERE ITE.NUNOTA = :NUNOTAORIG AND ITE.CODPROD = :CODPROD AND (ITE.CODVOL = :CODVOL OR ITE.AD_CODVOL = :CODVOL)),0) " +
                        " - NVL((SELECT SUM(CASE " +
                        "   WHEN ITE2.AD_CODVOL IS NULL AND ITE2.CODVOL = PRO2.CODVOL THEN ITE2.QTDNEG " +
                        "   WHEN ITE2.AD_CODVOL IS NULL AND ITE2.CODVOL <> PRO2.CODVOL THEN ITE2.QTDNEG / NVL(VOA3.QUANTIDADE,1) " +
                        "   WHEN ITE2.AD_CODVOL IS NOT NULL AND ITE2.AD_CODVOL = PRO2.CODVOL THEN ITE2.QTDNEG " +
                        "   WHEN ITE2.AD_CODVOL IS NOT NULL AND ITE2.AD_CODVOL <> PRO2.CODVOL THEN ITE2.QTDNEG / NVL(VOA4.QUANTIDADE,1) END) " +
                        "   FROM TGFCAB CAB2 JOIN TGFITE ITE2 ON ITE2.NUNOTA = CAB2.NUNOTA " +
                        "   JOIN TGFPRO PRO2 ON PRO2.CODPROD = ITE2.CODPROD " +
                        "   LEFT JOIN TGFVOA VOA3 ON VOA3.CODPROD = ITE2.CODPROD AND VOA3.CODVOL = ITE2.CODVOL " +
                        "   LEFT JOIN TGFVOA VOA4 ON VOA4.CODPROD = ITE2.CODPROD AND ITE2.AD_CODVOL = VOA4.CODVOL " +
                        "   WHERE CAB2.NUNOTA = :NUNOTACONF AND ITE2.CODBARRAPDV IS NOT NULL AND ITE2.CODPROD = :CODPROD AND (ITE2.CODVOL = :CODVOL OR ITE2.AD_CODVOL = :CODVOL)),0)),0) AS QTDPENDENTE " +
                        "FROM DUAL"
        );
        sql2.setNamedParameter("NUNOTACONF", nuNotaConf);
        sql2.setNamedParameter("CODBARRA", codBarra);
        sql2.setNamedParameter("NUNOTAORIG", nuNotaOrig);
        sql2.setNamedParameter("CODPROD", codProd);
        sql2.setNamedParameter("CODVOL", codVolNovo);

        try (ResultSet rs2 = sql2.executeQuery()) {
            if (rs2.next()) {
                codBarraConferido = rs2.getBigDecimal("CODBARRAREPETIDO");
                codVolPedido     = rs2.getBigDecimal("CODVOLPEDIDO");
                qtdPendente      = rs2.getBigDecimal("QTDPENDENTE");
            }
        }
        return new BigDecimal[]{codBarraConferido, codVolPedido, qtdPendente};
    }

    private BigDecimal calcularQtdPendente(JdbcWrapper jdbc, BigDecimal nuNotaConf, BigDecimal nuNotaOrig,
                                           BigDecimal codProd, String codVolNovo) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql(
                "SELECT NVL(( NVL((SELECT SUM(CASE " +
                        "   WHEN I.AD_CODVOL IS NULL AND I.CODVOL = P.CODVOL THEN I.QTDNEG " +
                        "   WHEN I.AD_CODVOL IS NULL AND I.CODVOL <> P.CODVOL THEN I.QTDNEG / NVL(V1.QUANTIDADE,1) " +
                        "   WHEN I.AD_CODVOL IS NOT NULL AND I.AD_CODVOL = P.CODVOL THEN I.QTDNEG " +
                        "   WHEN I.AD_CODVOL IS NOT NULL AND I.AD_CODVOL <> P.CODVOL THEN I.QTDNEG / NVL(V2.QUANTIDADE,1) END) " +
                        "   FROM TGFITE I JOIN TGFPRO P ON P.CODPROD = I.CODPROD " +
                        "   LEFT JOIN TGFVOA V1 ON V1.CODPROD = I.CODPROD AND V1.CODVOL = I.CODVOL " +
                        "   LEFT JOIN TGFVOA V2 ON V2.CODPROD = I.CODPROD AND V2.CODVOL = I.AD_CODVOL " +
                        "   WHERE I.NUNOTA = :NUNOTAORIG AND I.CODPROD = :CODPROD AND (I.CODVOL = :CODVOL OR I.AD_CODVOL = :CODVOL)),0) " +
                        " - NVL((SELECT SUM(CASE " +
                        "   WHEN I2.AD_CODVOL IS NULL AND I2.CODVOL = P2.CODVOL THEN I2.QTDNEG " +
                        "   WHEN I2.AD_CODVOL IS NULL AND I2.CODVOL <> P2.CODVOL THEN I2.QTDNEG / NVL(V3.QUANTIDADE,1) " +
                        "   WHEN I2.AD_CODVOL IS NOT NULL AND I2.AD_CODVOL = P2.CODVOL THEN I2.QTDNEG " +
                        "   WHEN I2.AD_CODVOL IS NOT NULL AND I2.AD_CODVOL <> P2.CODVOL THEN I2.QTDNEG / NVL(V4.QUANTIDADE,1) END) " +
                        "   FROM TGFCAB C2 JOIN TGFITE I2 ON I2.NUNOTA = C2.NUNOTA " +
                        "   JOIN TGFPRO P2 ON P2.CODPROD = I2.CODPROD " +
                        "   LEFT JOIN TGFVOA V3 ON V3.CODPROD = I2.CODPROD AND V3.CODVOL = I2.CODVOL " +
                        "   LEFT JOIN TGFVOA V4 ON V4.CODPROD = I2.CODPROD AND I2.AD_CODVOL = V4.CODVOL " +
                        "   WHERE C2.NUNOTA = :NUNOTACONF AND I2.CODBARRAPDV IS NOT NULL AND I2.CODPROD = :CODPROD AND (I2.CODVOL = :CODVOL OR I2.AD_CODVOL = :CODVOL)),0)),0) AS PEND " +
                        "FROM DUAL"
        );
        sql.setNamedParameter("NUNOTACONF", nuNotaConf);
        sql.setNamedParameter("NUNOTAORIG", nuNotaOrig);
        sql.setNamedParameter("CODPROD", codProd);
        sql.setNamedParameter("CODVOL", codVolNovo);

        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("PEND");
        }
        return BigDecimal.ZERO;
    }

    private BigDecimal checarVolNoPedidoOriginal(JdbcWrapper jdbc, BigDecimal nuNotaOrig, BigDecimal codProd, String codVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql(
                "SELECT COUNT(*) QT " +
                        "  FROM TGFITE I " +
                        " WHERE I.NUNOTA = :NUNOTAORIG AND I.CODPROD = :CODPROD AND (I.CODVOL = :CODVOL OR I.AD_CODVOL = :CODVOL)"
        );
        ns.setNamedParameter("NUNOTAORIG", nuNotaOrig);
        ns.setNamedParameter("CODPROD", codProd);
        ns.setNamedParameter("CODVOL", codVol);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("QT");
        }
        return BigDecimal.ZERO;
    }

    private BigDecimal obterNuNotaOrig(JdbcWrapper jdbc, BigDecimal nuNotaConf) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql(
                "SELECT CON.NUNOTAORIG " +
                        "  FROM AD_FTICONFERENCIA CON " +
                        "  JOIN TGFCAB CAB2 ON CAB2.NUNOTA = CON.NUNOTACONF " +
                        " WHERE CAB2.NUNOTA = :NUNOTA"
        );
        sql.setNamedParameter("NUNOTA", nuNotaConf);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("NUNOTAORIG");
        }
        return null;
    }

    // =============================== Helpers Diversos ===============================

    private Integer obterQtdPorPalete(JdbcWrapper jdbc, BigDecimal codProd, String codVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT AD_QTDPALETE FROM TGFVOA WHERE CODPROD = :P1 AND CODVOL = :P2");
        ns.setNamedParameter("P1", codProd);
        ns.setNamedParameter("P2", codVol);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) {
                BigDecimal bd = rs.getBigDecimal(1);
                if (bd != null) return bd.intValue();
            }
        }
        return null;
    }

    private int contarItensIvc(JdbcWrapper jdbc, BigDecimal nuNota, BigDecimal codProd, String codVol, BigDecimal seqVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT COUNT(*) FROM TGFITE WHERE NUNOTA = :C1 AND CODPROD = :C2 AND CODVOL = :C3 AND AD_SEQVOL = :C4");
        ns.setNamedParameter("C1", nuNota);
        ns.setNamedParameter("C2", codProd);
        ns.setNamedParameter("C3", codVol);
        ns.setNamedParameter("C4", seqVol);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
        }
        return 0;
    }

    private BigDecimal obterTopDaNota(JdbcWrapper jdbc, BigDecimal nuNota) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT CODTIPOPER FROM TGFCAB WHERE NUNOTA = :NUNOTA");
        sql.setNamedParameter("NUNOTA", nuNota);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("CODTIPOPER");
        }
        return null;
    }

    private String obterImpressora(JdbcWrapper jdbc, BigDecimal codLocal) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT IMPRESSORA FROM AD_FTICONFERENCIAIMPRES WHERE CODLOCAL = :CODLOCAL");
        sql.setNamedParameter("CODLOCAL", codLocal);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getString("IMPRESSORA");
        }
        return "?";
    }

    private BigDecimal obterLocalDestino(JdbcWrapper jdbc, BigDecimal nuNota) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT NVL(CODLOCALDEST, CODLOCAL) DESTINO FROM AD_FTICONFERENCIA WHERE NUNOTACONF = :NUNOTACONF");
        sql.setNamedParameter("NUNOTACONF", nuNota);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("DESTINO");
        }
        return null;
    }

    private boolean existePaleteAtivoPorCodigo(JdbcWrapper jdbc, String cb) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT 1 FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB AND ATIVO='S'");
        ns.setNamedParameter("CB", cb);
        try (ResultSet rs = ns.executeQuery()) { return rs.next(); }
    }

    private boolean existePaleteQualquerPorCodigo(JdbcWrapper jdbc, String cb) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT 1 FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB");
        ns.setNamedParameter("CB", cb);
        try (ResultSet rs = ns.executeQuery()) { return rs.next(); }
    }

    private BigDecimal obterQtdPalete(JdbcWrapper jdbc, String codBarraPalete) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT QTD FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB");
        ns.setNamedParameter("CB", codBarraPalete);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("QTD");
        }
        return BigDecimal.ZERO;
    }

    private BigDecimal obterLocalDoPalete(JdbcWrapper jdbc, String codBarraPalete) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT CODLOCAL FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB AND ATIVO='S'");
        ns.setNamedParameter("CB", codBarraPalete);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("CODLOCAL");
        }
        return null;
    }
}
