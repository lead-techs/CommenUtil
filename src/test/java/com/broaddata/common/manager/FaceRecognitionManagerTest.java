/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.manager;

import com.broaddata.common.util.FileUtil;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author ed
 */
public class FaceRecognitionManagerTest {
 
 public FaceRecognitionManagerTest() {
 }
 
 @BeforeClass
 public static void setUpClass() {
 }
 
 @AfterClass
 public static void tearDownClass() {
 }
 
 @Before
 public void setUp() {
 }
 
 @After
 public void tearDown() {
 }

 
 /**
 * Test of compareTwoFaces method, of class FaceRecognitionManager.
 */
 @Ignore
 @Test
 public void testCompareTwoFaces() {
     System.out.println("compareTwoFaces");


     String str1 = "0.8737463 0.3193769 0.8579237 -0.8638373 -0.5505858 -0.95721364" +
    " 0.6803695 -0.69083893 -0.70987135 0.7850794 -0.16375951 0.35814232" +
    " -0.08895817 -0.7387241 -0.24288704 0.98283625 -0.2549459 -0.84935623" +
    " -0.68614966 0.23840462 -0.6499116 -0.45703918 0.5237325 0.82372344" +
    " 0.96381456 0.6219693 -0.09861911 -0.9469713 0.46219093 -0.8961866" +
    " 0.97377855 -0.4315639 0.9643258 0.5232351 -0.841304 0.972559" +
    " 0.67802703 0.34410942 -0.85281295 0.01957615 0.2699306 0.8554211" +
    " 0.5395165 -0.34424442 -0.8323078 -0.98500085 -0.10598441 0.9350149" +
    " 0.80830646 0.7387475 0.24452084 0.7662448 0.9610741 -0.99291277" +
    " 0.64100534 -0.6374331 -0.8364084 -0.52900636 0.5819015 -0.9253685" +
    " -0.86477333 -0.33172488 0.81639063 -0.8238884 -0.15436834 0.8360356" +
    " -0.914966 -0.9571469 -0.65925264 -0.9828249 -0.91224647 -0.670177" +
    " 0.98215955 -0.9773487 -0.89021575 0.47722366 -0.9443235 0.51053023" +
    " -0.9879222 0.99382627 -0.46616906 -0.95535177 -0.9047429 0.2731457" +
    " 0.888349 0.18598141 -0.6032761 -0.00131762 0.5890215 0.75230104" +
    " -0.8515751 -0.6608987 0.9404894 0.8536341 0.60295665 -0.70100486" +
    " -0.54594684 0.9886654 0.95230865 0.48462975 -0.9486907 -0.6064294" +
    " 0.5219097 -0.32954016 -0.9367325 -0.40383002 -0.08387902 -0.53707725" +
    " -0.8812589 0.93628615 0.9963764 0.8207706 0.3773663 -0.9041438" +
    " -0.80150217 0.2691379 0.9222281 0.6871711 -0.809687 0.05826596" +
    " -0.9524819 -0.6848957 0.5484583 -0.4364506 0.78676146 -0.8960009" +
    " 0.9862167 0.7489473 0.9530932 -0.7668552 0.49511886 -0.46052548" +
    " 0.9855331 -0.741241 0.77278084 0.9169872 -0.14624639 0.8129949" +
    " -0.8570101 -0.57645136 -0.98916197 -0.8309261 -0.6084912 -0.0154152" +
    " -0.0158945 -0.5908363 0.95632464 0.6284438 -0.4606841 0.94325197" +
    " 0.6961106 -0.21927597 -0.49542126 0.1987697 0.61300534 0.53973067" +
    " -0.40027726 0.9871726 0.94127953 0.43195006 0.9348043 0.35526788" +
    " -0.54169756 -0.84059596 0.0604814 -0.28623903 -0.99337137 0.7905342" +
    " -0.9654155 -0.6867976 0.43560916 -0.76091444 -0.8118685 0.92375255" +
    " -0.44348323 -0.97925013 -0.4977646 0.44720933 -0.16170405 0.7320295" +
    " -0.00855968 -0.51670873 0.35480073 -0.75807434 0.45042273 0.9642167" +
    " 0.9881801 0.8756169 0.87668365 0.20890379 -0.1821557 -0.25561875" +
    " 0.74214613 0.1667358 -0.8690645 0.11917383 0.5867544 -0.9916974" +
    " -0.9973899 0.7311541 -0.9401653 0.24007681 0.98646265 -0.6495797" +
    " -0.35561195 0.99009246 0.9214053 0.987157 -0.9969198 0.6905507" +
    " 0.2235625 -0.95115834 0.7685724 0.9161691 -0.6021192 0.07007135" +
    " -0.09290725 0.697387 0.963035 -0.9929512 0.9348139 0.96920526" +
    " 0.8989408 -0.9854913 -0.99046034 0.7186814 0.74135154 0.70331377" +
    " 0.2506142 -0.62421316 -0.9853422 0.5565598 -0.8876479 -0.9877717" +
    " 0.00354817 -0.24770647 0.85190153 0.94775736 0.05465376 -0.05144344" +
    " -0.7381919 0.81853133 -0.85583085 0.9135731 0.89941484 -0.3976694" +
    " 0.93985903 -0.9959264 0.01372422 0.72109354 -0.7729492 0.8919049" +
    " -0.9699639 0.88442475 -0.67133653 -0.9447676 -0.9284621 -0.31924245" +
    " -0.94013214 0.59074014 0.46194705 0.45244125 -0.98520845 0.50312364" +
    " 0.8498342 0.2719854 0.59467316 -0.9692451 -0.27113116 -0.7879879" +
    " -0.9275913 -0.77802396 0.02116095 -0.74682266 0.79835 0.7797464" +
    " -0.8537849 0.4720951 0.8427805 -0.8263403 -0.76358473 0.5818237" +
    " -0.98988396 -0.8492369 -0.72366273 0.44233045 -0.796083 0.9788396" +
    " 0.9702655 0.7812329 -0.05180644 -0.9241092 0.97305954 -0.55607647" +
    " 0.8944405 0.9566809 -0.9631527 0.96018106 0.74419355 0.276912" +
    " 0.89436567 0.47899112 -0.575211 -0.83616537 0.5181159 0.9288299" +
    " -0.9776088 -0.5662849 -0.7273265 -0.2835652 -0.9866771 -0.9770024" +
    " -0.9725164 -0.82036173 0.5884544 0.97622186 0.9353057 -0.00874341" +
    " -0.99741864 0.6686315 -0.22631197 -0.978433 0.9478079 0.47621793" +
    " 0.9142959 -0.74781775 0.7718152 -0.79853785 -0.95262724 0.5436055" +
    " 0.54339844 0.82501364 0.7618431 0.8638391 -0.9889105 0.6956068" +
    " 0.5531873 -0.6361686 -0.98485136 -0.93687624 -0.8532314 0.8119365" +
    " -0.39381924 -0.44919983 0.07779353 -0.99406564 0.90927476 0.80156267" +
    " -0.95251226 0.747337 -0.7637733 0.69929385 -0.7945645 0.33682057" +
    " 0.9626863 0.02751832 -0.7089288 -0.42217982 0.3854859 -0.9891199" +
    " -0.38379905 0.19533224 -0.85840446 0.3562326 -0.7167159 0.35530812" +
    " -0.96703714 0.8492996 -0.3563015 0.92521834 -0.8761408 0.9731288" +
    " 0.97376907 0.950553 0.7842409 -0.97088045 -0.15556341 -0.8990283" +
    " 0.75833106 0.9348998 -0.89277536 -0.8534993 -0.21599953 0.9767846" +
    " 0.46715 -0.8441942 0.95252347 -0.92177653 0.03638702 0.17058511" +
    " 0.99439996 -0.87808114 -0.41393542 0.99003655 -0.98073196 0.0919574" +
    " 0.9881729 -0.7472644 0.8125125 0.9027097 0.9259951 0.4588493" +
    " 0.54923654 0.3660885 0.46510652 0.21083505 0.22007607 -0.95516086" +
    " 0.93066293 0.9444217 -0.8475582 0.77486855 0.20242035 0.20833372" +
    " 0.21806137 -0.95891935 -0.00707101 -0.94643927 -0.49726808 0.9620287" +
    " -0.9151581 -0.86342967 0.81732124 0.7118389 -0.03229137 -0.47753203" +
    " -0.2545821 0.63651806 0.84687775 -0.4993819 -0.39545485 -0.75155115" +
    " -0.9316501 0.7766244 -0.5009725 -0.98495615 -0.9513288 -0.7367464" +
    " 0.9038972 -0.7101605 0.99623084 -0.48322842 0.7904676 0.8715725" +
    " 0.92780364 0.15166631 0.65834445 0.9939883 -0.15563259 -0.9709644" +
    " 0.9650531 -0.01725011 -0.9747047 -0.05895137 -0.48592108 -0.8762941" +
    " -0.42453235 0.34807384 -0.9870429 -0.6827269 -0.12653928 0.585607" +
    " 0.9239407 -0.9430473 -0.8914277 -0.11479524 0.8785315 0.47969803" +
    " -0.9906617 0.79622626 0.10394938 0.419163 -0.27055085 0.751587" +
    " 0.5104437 0.40849578 0.91049445 0.90869963 -0.10982967 -0.75580734" +
    " -0.5167438 0.4221382 0.36329773 -0.96457344 -0.36324403 0.8188814" +
    " -0.70077246 -0.4406155 -0.7607875 0.86493975 -0.5027666 0.89950454" +
    " 0.43498698 -0.12601165 -0.50602144 -0.72731036 0.33619133 -0.7643302" +
    " -0.25840464 -0.91132444 -0.06302218 0.84293455 0.84251237 0.62877786" +
    " 0.98436874 0.88227355 -0.52201605 -0.9924124 0.05590573 -0.5897673" +
    " -0.414923 -0.99343944";

     /*

     String str1 = "0.7897713 -0.09112068 0.4754625 -0.8562494 -0.96023256 -0.5532654" +
    " 0.7914293 -0.47383863 -0.7696708 0.53846955 -0.44274467 -0.6070996" +
    " -0.6248837 -0.15300804 -0.8432532 0.99107474 -0.45266837 -0.53365153" +
    " -0.8148159 0.70575464 -0.6416418 -0.681888 0.05115555 0.89088637" +
    " 0.99219275 0.7688091 0.6025856 -0.95250696 0.3542321 -0.7103353" +
    " 0.98133737 -0.22346346 0.97395396 0.47588313 -0.52577615 0.97903043" +
    " 0.7557416 0.8831457 -0.72080535 -0.737136 0.7665931 0.86772484" +
    " -0.6078575 -0.64244246 -0.19435257 -0.96572614 -0.5686988 0.9090784" +
    " 0.9631691 0.771651 0.46137714 -0.03179993 0.9463654 -0.98539865" +
    " 0.79700387 -0.8553398 -0.8572461 0.11156953 0.89809304 -0.893243" +
    " -0.67875296 0.2778442 0.51044875 -0.95019186 0.82438815 -0.03954239" +
    " -0.726032 -0.881502 -0.5812065 -0.852168 -0.83537775 -0.26270196" +
    " 0.5632255 -0.9411164 -0.95194596 0.70560503 -0.9701032 0.6754711" +
    " -0.96469665 0.8939955 -0.5215463 -0.8224808 -0.9372483 -0.50744104" +
    " 0.85653627 0.8833544 -0.2346761 0.68933237 0.21394438 0.8982392" +
    " -0.7255832 -0.52151304 0.959119 0.5943243 0.6692942 -0.0859562" +
    " -0.6597696 0.93261003 0.9927327 -0.6625868 -0.69918025 -0.679684" +
    " -0.13575535 -0.66371286 -0.9942209 -0.41264594 -0.5566444 -0.92647713" +
    " -0.74690783 0.9270082 0.9986065 0.84919137 0.7494845 -0.97344726" +
    " -0.8957145 -0.39629593 0.8775299 0.91686195 -0.8294228 0.4077873" +
    " -0.9747862 -0.65812135 0.3203125 -0.4957665 0.85472673 -0.7682211" +
    " 0.98064065 -0.19748138 0.00869055 0.52514243 -0.3405451 0.67614233" +
    " 0.96984786 -0.87040037 0.12200041 0.96297854 -0.06809225 0.41771197" +
    " -0.83233374 0.4354064 -0.92135996 -0.9238886 -0.9063694 0.28285316" +
    " 0.00124201 -0.7175002 0.4818959 0.6303217 -0.44085485 0.96537787" +
    " 0.9316444 -0.36617303 0.17176871 0.5267937 0.22271796 0.94650537" +
    " -0.13956887 0.9484171 0.4441535 -0.5874934 0.94513077 0.67077345" +
    " -0.65515494 0.29231223 -0.5138343 -0.35370937 -0.9535233 0.9241071" +
    " -0.98874056 -0.5132645 0.46171758 -0.92146796 0.21136406 0.9043222" +
    " 0.16536923 -0.86078346 -0.89756864 0.71666545 -0.7469403 -0.35187554" +
    " 0.09118124 -0.635075 -0.6763575 -0.9230088 -0.81016356 0.9676588" +
    " 0.93335336 0.64335746 0.9720303 -0.6077666 -0.24369055 -0.23460922" +
    " 0.64422846 0.06776785 -0.544132 0.58353704 0.81656665 -0.9913736" +
    " -0.9504735 0.84518874 -0.9002557 0.8477146 0.97987473 -0.03371429" +
    " -0.8603254 0.9654217 0.9704096 0.92563796 -0.9897028 0.9175179" +
    " 0.5141783 -0.9515888 0.7904078 0.1207825 -0.9697983 -0.16493659" +
    " 0.13977271 0.81099886 0.93991125 -0.97387075 0.6880416 0.7532812" +
    " 0.9756525 -0.96279573 0.5931714 0.81790197 0.67116463 0.9871332" +
    " -0.5348687 -0.7297313 -0.9616802 0.35597333 -0.8821808 -0.96563256" +
    " -0.9516227 -0.31938046 0.9800004 0.89285433 -0.8563943 -0.6097902" +
    " 0.7164187 0.9141381 -0.5815023 0.9667844 0.61457205 -0.83644944" +
    " 0.9573759 -0.94840187 0.36199 -0.11748412 -0.70360917 0.8765035" +
    " -0.9684472 0.96255296 -0.4783247 -0.419169 -0.68523955 -0.5249405" +
    " -0.9672303 0.55863416 -0.7888293 0.46639195 -0.9904813 0.87219405" +
    " 0.8946664 0.28907084 -0.57824755 -0.21884659 -0.14529419 -0.6462389" +
    " -0.86145 -0.8891111 -0.4763814 -0.25715524 0.8575118 0.8634967" +
    " -0.7626362 0.88761795 0.39267632 -0.49493596 0.38350892 0.6497901" +
    " -0.9723532 -0.8598291 -0.4660653 0.8447982 -0.9908736 0.9162186" +
    " 0.9138623 0.6813528 -0.588375 -0.59084547 0.9932943 -0.92274535" +
    " 0.5733954 0.91478884 -0.8794992 0.93709546 0.5667644 0.79311615" +
    " 0.91460073 0.68716395 -0.40866756 -0.9191487 0.86544394 0.71663594" +
    " -0.9778804 -0.28894624 0.7629269 0.0352708 -0.41532844 -0.96990746" +
    " -0.63259184 -0.91005385 0.8280214 0.98733157 0.9219706 0.17689957" +
    " -0.9882096 0.28238508 -0.51176107 -0.9834395 0.97100663 0.8010478" +
    " 0.96700996 -0.93332815 0.3751717 -0.45239466 -0.45966676 0.24754097" +
    " 0.4916329 0.4238374 0.88808084 0.19074285 -0.8728494 0.57194173" +
    " 0.47460946 -0.5931916 -0.98645175 -0.8941027 -0.81591177 0.72504336" +
    " 0.3393546 0.73426557 -0.52493215 -0.94138175 0.70752734 0.97212064" +
    " -0.95121515 0.22889864 0.07041268 0.8723482 0.7373175 -0.23048276" +
    " 0.9787172 0.78972435 -0.8144403 -0.53648895 -0.45591167 -0.99778086" +
    " -0.7566334 -0.67747605 0.5876784 0.7591879 -0.26513892 0.85756147" +
    " -0.96753716 -0.33260238 0.15519173 0.9068546 -0.9147142 -0.06724763" +
    " 0.9293615 0.7594261 0.64981496 -0.8450149 -0.5713479 -0.8644414" +
    " 0.17793302 0.6835098 -0.7506521 -0.89678836 -0.8975227 0.9833097" +
    " -0.7348602 -0.8000617 0.9471089 -0.61630446 0.88046 0.41007775" +
    " 0.9959466 -0.80851644 -0.5976445 0.9715748 -0.9517758 0.01628875" +
    " 0.99007195 -0.74622536 0.88009185 0.32789978 0.8662586 0.21101828" +
    " 0.30602255 0.47821483 0.8163678 0.27907777 0.662484 -0.8988358" +
    " 0.8013794 0.9439482 -0.96727157 0.8849905 -0.15203767 0.49075213" +
    " 0.88919115 -0.9156695 0.3438413 -0.9596929 -0.7028179 0.90455335" +
    " -0.90351814 -0.37947285 0.9636189 0.08593071 0.7051484 -0.55110806" +
    " 0.52629423 0.86570567 0.86421883 -0.25416356 -0.49445233 -0.39777744" +
    " -0.74188095 0.8159532 -0.04578308 -0.9730413 -0.92793185 -0.8853398" +
    " 0.88172615 -0.6163155 0.99141365 -0.2085697 0.49501655 0.793962" +
    " -0.19990185 -0.46363628 0.29484957 0.9891175 -0.68033034 -0.98664254" +
    " 0.7271373 -0.08345243 -0.9380855 -0.5664746 -0.6216976 -0.9947221" +
    " 0.08287129 0.9111404 -0.98891014 0.81688005 -0.17078535 0.8462009" +
    " 0.9109806 -0.04513327 -0.9712954 -0.28555232 0.7554096 0.9885835" +
    " -0.996042 0.78851545 0.5388949 -0.49269354 -0.48177448 -0.60433406" +
    " 0.68483996 0.4963029 0.27826297 0.23950745 -0.0011221 0.51597714" +
    " -0.09533831 0.7227213 0.79613304 -0.95186865 -0.2652082 0.9799217" +
    " -0.92393404 0.59697104 -0.8296842 0.830045 -0.38506165 0.4368917" +
    " -0.4915449 -0.1393921 -0.42413357 0.09589498 -0.08806314 -0.27736178" +
    " 0.36937207 -0.7554616 0.07476812 0.23581228 0.9791022 0.4578604" +
    " 0.9750653 -0.6666935 0.54060566 -0.9858636 -0.2295693 -0.35253835" +
    " 0.533635 -0.97979647";*/


    String str2 = "0.8806661 0.5012645 0.89831316 -0.8002778 -0.04467971 -0.95453805" +
    " 0.04422018 -0.49677983 -0.77577364 0.8131175 -0.13867457 0.46349788" +
    " -0.25498134 -0.8817235 0.11613747 0.9841799 0.32087564 -0.73356" +
    " -0.6059052 -0.00417603 -0.72327733 -0.42027867 0.2282784 0.49608353" +
    " 0.94174284 0.7642798 0.30485943 -0.9084085 0.4612504 -0.954198" +
    " 0.9818337 0.11457872 0.95114225 0.4524668 -0.8516343 0.9588403" +
    " 0.44221297 0.22985026 -0.78958845 -0.17762303 0.13734254 0.6259877" +
    " 0.4169385 -0.3691887 -0.84578806 -0.97682077 -0.12912637 0.9354545" +
    " 0.75591564 0.8780201 -0.1387232 0.80890465 0.90027344 -0.9954116" +
    " 0.4611961 -0.56090623 -0.7957669 -0.7119796 0.65608525 -0.9028293" +
    " -0.9528263 -0.8941826 0.5028265 -0.67228717 0.22923948 0.9160748" +
    " -0.84104735 -0.89334095 0.91353476 -0.41503483 0.00148064 -0.07018099\" +\n" +
    "\" -0.91039157 0.958-0.60245097 -0.9518984 -0.92844105 -0.32709494" +
    " 0.9803231 -0.9800167 -0.60274386 0.29263315 -0.9447226 0.5508938" +
    " -0.95043385 0.99522346 -0.21646513 -0.96749103 -0.73038393 0.5214603" +
    " 0.8955687 0.4623241 -0.8756976 -0.07568517 0.40101716 0.67641574" +
    " -0.83852917 -0.6072364 0.97218895 0.89614075 -0.11846116 -0.46096018" +
    " -0.82593393 0.9520271 0.880717 0.24608205 -0.7865871 -0.22428383" +
    " 0.5435947 0.09101195 -5633 0.99714434 0.81227446 0.30561215 -0.90414137" +
    " -0.8106653 0.71202415 0.9530301 0.6395834 -0.43110493 0.17433484" +
    " -0.96873605 -0.73928785 0.41688097 -0.73575854 0.7313853 -0.911926" +
    " 0.9714271 0.69080997 0.95631665 -0.87209094 0.37451383 -0.16836087" +
    " 0.9728817 -0.8694654 0.84631306 0.9519783 -0.09661214 0.7990963" +
    " -0.89831114 -0.5772568 -0.9796347 -0.81855065 -0.3817981 0.21336368" +
    " 0.6912352 -0.23752134 0.9522291 0.6006061 -0.31358626 0.886317" +
    " 0.36788237 -0.4902004 -0.05345847 0.24138577 0.60023654 0.6934681" +
    " -0.24702075 0.9904062 0.94727266 0.67154336 0.87924826 0.25771323" +
    " -0.31518736 -0.9739501 -0.31184307 -0.1783458 -0.99200815 0.7643842" +
    " -0.96192014 -0.5360793 0.6133988 -0.27900916 -0.4443509 0.84049124" +
    " -0.71975166 -0.93532056 -0.4479574 0.9177534 0.16332048 0.87431407" +
    " -0.03069745 -0.33120692 0.7378405 -0.72128797 0.47099555 0.9500741" +
    " 0.9828016 0.7897886 0.9253907 0.14855723 -0.76880634 -0.2805211" +
    " 0.52478397 0.6060871 -0.82713526 0.28662208 0.2716071 -0.9900411" +
    " -0.9900757 0.8422855 -0.9662376 -0.07027853 0.9842503 -0.67999846" +
    " -0.16387866 0.9888552 0.9292643 0.9824363 -0.9903136 0.57838774" +
    " 0.01998831 -0.8898435 0.7338348 0.9177648 -0.67200154 0.1647265" +
    " -0.13760337 0.5105324 0.8881599 -0.9948073 0.96286845 0.9306063" +
    " 0.9365466 -0.9826601 -0.9767386 0.5159578 0.5221033 0.68301755" +
    " 0.12991573 -0.63126695 -0.9917063 0.1627304 -0.7787835 -0.9872206" +
    " -0.2481872 -0.33110687 0.9260254 0.92912304 0.466634 0.16752808" +
    " -0.82728225 0.90671885 -0.7072969 0.8945307 0.82775605 -0.38733745" +
    " 0.88558155 -0.9940374 0.19261885 0.68119353 -0.7028911 0.9501589" +
    " -0.91711557 0.8226443 -0.6326813 -0.9268584 -0.89260507 -0.39408705" +
    " -0.9507545 0.61827755 0.10575218 0.37173983 -0.9859156 0.6601962" +
    " 0.9167929 0.67713386 0.43223527 -0.97303253 0.01710045 -0.7353741" +
    " -0.9024265 -0.76533735 0.09162036 -0.6241078 0.56794405 0.631051" +
    " -0.85835296 0.21928266 0.7740221 -0.7129307 -0.56125605 0.3977818" +
    " -0.9779696 -0.893504 -0.44725138 0.32674363 -0.8629274 0.9658717" +
    " 0.969559 0.82273394 -0.01014541 -0.9459067 0.9765919 -0.57249844" +
    " 0.89084566 0.9728422 -0.9514446 0.93621695 0.47077927 -0.1757485" +
    " 0.8170225 0.34080994 -0.6613209 -0.6509794 0.6769879 0.9345821" +
    " -0.97912455 -0.33452004 -0.28103456 -0.64267665 -0.98598886 -0.9685856" +
    " -0.95860445 -0.73250926 0.5860998 0.9792701 0.8954397 -0.1796681" +
    " -0.99648285 0.39867878 -0.42835745 -0.9834536 0.9626839 0.3748886" +
    " 0.9355318 -0.9002507 0.61483604 -0.9038493 -0.9569172 0.5993181" +
    " 0.7461498 0.64881593 0.86204976 0.82703674 -0.9800418 0.48707107" +
    " 0.8024526 -0.710672 -0.98592305 -0.93865377 -0.8911077 0.7478563" +
    " -0.7312566 -0.21972448 0.1436699 -0.9953716 0.89222735 0.47403076" +
    " -0.8630796 0.8847154 -0.71403795 0.84582037 -0.7055787 0.2671759" +
    " 0.96408963 0.41392204 -0.5245783 0.17806691 -0.08466656 -0.9854125" +
    " 0.18280579 -0.05247319 -0.8963017 0.1067955 -0.5394329 0.5208845" +
    " -0.91982704 0.8505716 -0.21916573 0.9714279 -0.8826284 0.9822984" +
    " 0.9591643 0.96965563 0.32998925 -0.9711322 -0.11319467 -0.9211033" +
    " 0.68731886 0.91388905 -0.85555696 -0.65682656 -0.071027 0.97812396" +
    " 0.5464455 -0.91128755 0.94455075 -0.97959167 0.09750392 -0.20191544" +
    " 0.99310577 -0.76202154 -0.7456128 0.9633849 -0.92897683 0.14522105" +
    " 0.98276776 -0.7668119 0.42221403 0.8944222 0.9173817 0.6740451" +
    " 0.61156446 0.08867411 0.31923702 0.10499679 -0.0813689 -0.9198686" +
    " 0.8833952 0.91645855 -0.5952712 0.78145 0.2009438 0.12479743" +
    " 0.11960692 -0.9510852 0.5364052 -0.9641793 -0.4334341 0.90960866" +
    " -0.9457243 -0.9356424 0.64803576 0.27569783 0.04247267 -0.4570605" +
    " 0.06216317 0.65850914 0.92484814 -0.32541668 -0.4018029 -0.515679" +
    " -0.9225465 0.50906086 -0.7693612 -0.95889497 -0.94909453 -0.7447736" +
    " 0.93952787 0.00818747 0.9932088 -0.69120353 0.89964193 0.8536675" +
    " 0.9251175 -0.10937507 0.43246573 0.9938127 -0.06014789 -0.9268041" +
    " 0.9793696 -0.01870066 -0.8866465 0.19876683 -0.5366516 -0.9942837" +
    " -0.47266328 0.5154129 -0.97356886 -0.5915359 -0.06991818 0.7444551" +
    " 0.9674118 -0.8723707 -0.7468642 0.20932631 0.84683645 0.7861714" +
    " -0.9943763 0.70519274 -0.36091638 0.1961528 -0.23718297 0.57249457" +
    " 0.46022683 0.6918683 0.87299013 0.9061566 0.17033774 -0.65140104" +
    " -0.80191785 0.17410444 0.4754943 -0.9831979 -0.66684437 0.7048644" +
    " -0.7344243 -0.36093673 -0.6718214 0.7886011 -0.77417845 0.7822996" +
    " 0.6338326 -0.15813556 -0.6345605 -0.9523294 0.34318727 -0.7121414" +
    " 0.1170241 -0.9061312 -0.03388413 0.89230114 0.41601723 0.5845516" +
    " 0.98432374 0.8128999 -0.61869717 -0.99380964 0.58237773 -0.49567506" +
    " -0.4979094 -0.979386"; 
    /*
    String str2 = "7.09684193e-01 5.39643466e-01 7.69645989e-01 -8.02119017e-01" +
    " -9.14253592e-01 -7.54384756e-01 7.59178638e-01 -3.88872653e-01" +
    " -7.66204357e-01 5.85098267e-01 -2.87194163e-01 -5.98723948e-01" +
    " -5.34204125e-01 -4.06970412e-01 -7.66819775e-01 9.90671515e-01" +
    " -1.76913030e-02 -6.77456617e-01 -9.15888071e-01 7.53574729e-01" +
    " -7.86210299e-01 -7.68055260e-01 -1.99561864e-01 8.06040347e-01" +
    " 9.83871996e-01 6.71859920e-01 7.39222407e-01 -9.59064305e-01" +
    " 4.07784611e-01 -8.64292145e-01 9.62334871e-01 1.71269000e-01" +
    " 9.76532280e-01 4.63275880e-01 -7.31754124e-01 9.73850965e-01" +
    " 6.38250649e-01 8.42014909e-01 -8.79336596e-01 -6.55569971e-01" +
    " 8.06656420e-01 5.96119106e-01 -4.85553503e-01 -2.19145700e-01" +
    " -2.82128274e-01 -9.69639301e-01 -2.92158931e-01 9.55824494e-01" +
    " 9.52798605e-01 7.81760871e-01 6.07679188e-01 -3.19912285e-01" +
    " 8.53907347e-01 -9.92130637e-01 6.60982192e-01 -7.87296534e-01" +
    " -7.43543565e-01 1.46124318e-01 7.55065382e-01 -9.13365960e-01" +
    " -7.87994981e-01 1.33404717e-01 1.87165380e-01 -9.37371194e-01" +
    " 6.64781868e-01 4.71522182e-01 -5.60615897e-01 -9.65699375e-01" +
    " -6.62794054e-01 -6.27706945e-01 -7.98698485e-01 -3.03284317e-01" +
    " 2.05706209e-01 -9.65381622e-01 -9.34733808e-01 7.68438935e-01" +
    " -9.79259729e-01 8.04076612e-01 -9.68348682e-01 9.60268259e-01" +
    " -4.57219213e-01 -8.94130170e-01 -8.69173646e-01 -3.32043134e-02" +
    " 8.46305251e-01 8.77261102e-01 2.27414802e-01 7.40305007e-01" +
    " 1.86937690e-01 8.29938531e-01 -8.48774433e-01 -3.36128384e-01" +
    " 9.61824417e-01 7.68897653e-01 2.24239260e-01 -6.17608190e-01" +
    " -7.83523321e-01 8.81634653e-01 9.89665151e-01 -6.58595562e-01" +
    " -7.73947537e-01 -4.74693805e-01 1.69929489e-01 -9.45803151e-03" +
    " -9.97259676e-01 -8.23696911e-01 -6.32964492e-01 -8.70216191e-01" +
    " -8.03136468e-01 8.45755696e-01 9.98273790e-01 6.96818411e-01" +
    " 8.01154852e-01 -9.79165256e-01 -9.29427803e-01 4.10041623e-02" +
    " 8.70797038e-01 6.39218032e-01 -7.37867117e-01 6.55086577e-01" +
    " -9.78691816e-01 -9.17129815e-01 3.00061673e-01 -4.10325229e-01" +
    " 8.83387208e-01 -7.42252707e-01 9.89535570e-01 3.79362106e-01" +
    " 6.67828321e-01 -5.98281145e-01 -7.17449903e-01 6.03380620e-01" +
    " 9.67513859e-01 -9.06692863e-01 3.60450558e-02 9.07930434e-01" +
    " -2.01248795e-01 1.82628423e-01 -8.74718308e-01 -1.07089154e-01" +
    " -9.48619246e-01 -8.61373901e-01 -8.90743852e-01 6.59041762e-01" +
    " 3.03960800e-01 -7.72216439e-01 8.33875299e-01 2.09971219e-01" +
    " -1.79789960e-01 9.55528975e-01 6.30268574e-01 -4.29195315e-01" +
    " 4.15259719e-01 9.97488797e-02 -5.69445528e-02 9.54548597e-01" +
    " 1.30602689e-02 9.64155912e-01 3.01698387e-01 -7.83894479e-01" +
    " 8.34261179e-01 5.78310192e-01 -5.77642441e-01 1.12296849e-01" +
    " -5.73905587e-01 -5.79856932e-01 -9.84538555e-01 8.88698220e-01" +
    " -9.80385602e-01 -6.60117924e-01 6.20311439e-01 -9.09410775e-01" +
    " 3.40735286e-01 8.82310331e-01 -3.65977019e-01 -9.07176673e-01" +
    " -6.67725921e-01 7.73407876e-01 -4.94804770e-01 -3.54568720e-01" +
    " 1.70063004e-01 -3.63726646e-01 -3.66647720e-01 -9.07705605e-01" +
    " -6.87740684e-01 9.55479681e-01 9.24266279e-01 5.13313234e-01" +
    " 9.83623445e-01 -4.18406665e-01 -5.03397286e-01 -2.02531427e-01" +
    " 8.28751385e-01 -1.67947467e-02 -6.46915436e-01 1.97224557e-01" +
    " 6.63687825e-01 -9.88293707e-01 -9.77188945e-01 8.08228016e-01" +
    " -9.21456814e-01 6.65532708e-01 9.87527013e-01 -7.57411995e-04" +
    " -8.05310190e-01 9.77801442e-01 9.71527338e-01 9.40032065e-01" +
    " -9.91006672e-01 8.66243899e-01 -2.26588815e-01 -9.83669817e-01" +
    " 8.22358191e-01 4.24973249e-01 -9.61792827e-01 -1.04581073e-01" +
    " -1.84073105e-01 7.51124322e-01 9.21512663e-01 -9.78070676e-01" +
    " 8.40849459e-01 9.37355638e-01 9.73876238e-01 -9.17524815e-01" +
    " -2.33277261e-01 7.33513653e-01 8.68349373e-01 9.57743227e-01" +
    " -5.46529174e-01 -7.58069754e-01 -9.08874512e-01 7.23091424e-01" +
    " -8.11472714e-01 -9.77151513e-01 -9.31748807e-01 -7.45333806e-02" +
    " 9.76332188e-01 8.69251728e-01 -9.03890610e-01 -2.56755322e-01" +
    " 8.24563622e-01 9.10519481e-01 1.69607282e-01 9.70210969e-01" +
    " 8.49961460e-01 -8.24181318e-01 9.36152875e-01 -9.80930090e-01" +
    " 5.24901330e-01 1.29337445e-01 -7.43310153e-01 9.23241973e-01" +
    " -9.48492050e-01 9.03608024e-01 -5.62744439e-01 -3.01321745e-01" +
    " -9.32231843e-01 -5.95280468e-01 -9.75391865e-01 2.90657431e-01" +
    " -6.99977279e-01 -5.36802113e-02 -9.86707687e-01 8.94286096e-01" +
    " 9.51980650e-01 3.79386067e-01 -7.07248867e-01 -6.05387151e-01" +
    " -4.11848843e-01 -6.44523680e-01 -8.13326120e-01 -6.75326765e-01" +
    " 1.45759508e-01 -5.95848143e-01 6.48516238e-01 7.74501860e-01" +
    " -8.40615869e-01 8.61557841e-01 5.39919376e-01 4.19937819e-01" +
    " 7.22555697e-01 6.29962564e-01 -9.63093162e-01 -7.60974884e-01" +
    " 2.74910331e-02 8.34302604e-01 -9.86615300e-01 9.33009744e-01" +
    " 8.70350778e-01 7.54859567e-01 -1.86650544e-01 -4.79443997e-01" +
    " 9.87242460e-01 -8.78789663e-01 3.69197607e-01 9.60968852e-01" +
    " -8.98580015e-01 9.35514688e-01 6.02689326e-01 6.86802268e-01" +
    " 8.29246223e-01 5.91334403e-01 -5.87432384e-01 -9.11242366e-01" +
    " 8.93890023e-01 8.65664124e-01 -9.74050522e-01 -5.60371995e-01" +
    " 9.08160448e-01 -5.42352319e-01 -4.41157192e-01 -9.36365604e-01" +
    " -6.45986915e-01 -8.65486145e-01 9.19824600e-01 9.91678774e-01" +
    " 8.38893116e-01 -1.15789473e-01 -9.88139927e-01 2.76169479e-01" +
    " -9.67134163e-02 -9.86410618e-01 9.75978613e-01 7.80534565e-01" +
    " 9.41855192e-01 -9.21587706e-01 6.64593101e-01 -7.89473355e-01" +
    " -5.73385656e-01 3.32899064e-01 2.32954159e-01 8.07406008e-01" +
    " 8.68264854e-01 6.49977088e-01 -9.14474010e-01 4.90491182e-01" +
    " 3.67307305e-01 -4.91904914e-01 -9.95092154e-01 -8.61300409e-01" +
    " -9.13376391e-01 7.52215445e-01 -2.76759803e-01 4.92404491e-01" +
    " 3.43884856e-01 -9.87890601e-01 9.02244270e-01 9.17162299e-01" +
    " -9.57159638e-01 5.78499615e-01 -1.87468871e-01 9.00393605e-01" +
    " 3.68711650e-01 -4.46896106e-01 9.47699606e-01 6.28680825e-01" +
    " -4.72443163e-01 -2.58361340e-01 -3.74557257e-01 -9.92047131e-01" +
    " -8.76053393e-01 -6.82484269e-01 2.58588910e-01 4.80995029e-01" +
    " -3.87127340e-01 9.17752743e-01 -8.31496835e-01 2.22906664e-01" +
    " 4.60438877e-01 9.07655895e-01 -8.34848046e-01 9.08694714e-02" +
    " 9.70751703e-01 7.98189521e-01 3.75561178e-01 -8.08703601e-01" +
    " -5.18821776e-01 -7.11620331e-01 6.42196313e-02 9.06189978e-01" +
    " -7.79051900e-01 -9.21985149e-01 -8.27027678e-01 9.88650739e-01" +
    " -6.38120592e-01 -8.59404206e-01 9.54346299e-01 -8.34407210e-01" +
    " 7.10791647e-01 4.65224683e-01 9.95936811e-01 -7.78595448e-01" +
    " -8.75631571e-01 9.71462846e-01 -9.75292563e-01 8.14410001e-02" +
    " 9.94136035e-01 -9.16049361e-01 9.05066133e-01 2.76016086e-01" +
    " 8.75853777e-01 3.67866904e-01 3.64991158e-01 4.89147693e-01" +
    " 8.67260516e-01 1.81754649e-01 3.79147887e-01 -9.37405646e-01" +
    " 5.09353042e-01 9.43893731e-01 -9.62766171e-01 8.83671582e-01" +
    " 3.62407595e-01 5.54409087e-01 7.06672370e-01 -9.54901338e-01" +
    " 5.76591849e-01 -9.75533843e-01 -6.55718386e-01 8.76829147e-01" +
    " -7.69001782e-01 -3.25406373e-01 9.73969877e-01 1.63526744e-01" +
    " 5.59907854e-01 -3.76185715e-01 6.40224755e-01 9.29783642e-01" +
    " 9.25390303e-01 -3.61593425e-01 -5.94258368e-01 -7.18283594e-01" +
    " -9.38046277e-01 8.22046638e-01 -2.32723370e-01 -9.91609395e-01" +
    " -8.88191223e-01 -9.32340443e-01 8.62707853e-01 -6.25044331e-02" +
    " 9.88739073e-01 -4.45355058e-01 6.25215471e-01 7.43704438e-01" +
    " -1.75989106e-01 -5.46459019e-01 4.12681559e-03 9.90894020e-01" +
    " -6.36676490e-01 -9.83583987e-01 8.59551311e-01 -7.53396153e-02" +
    " -9.61844385e-01 -4.32505459e-01 -6.90551758e-01 -9.97305989e-01" +
    " 7.82653987e-02 8.46281767e-01 -9.87358034e-01 8.42204213e-01" +
    " -1.13074563e-01 7.99429238e-01 9.84520793e-01 -2.92199135e-01" +
    " -9.35873389e-01 -2.76345432e-01 8.51962864e-01 8.75319719e-01" +
    " -9.95079279e-01 7.77851462e-01 -3.42500657e-02 -2.09310248e-01" +
    " -4.72859651e-01 -1.59458444e-03 7.18322158e-01 6.43547237e-01" +
    " 2.30204776e-01 4.45640057e-01 9.90407243e-02 2.48641208e-01" +
    " -3.00744146e-01 7.53545284e-01 8.83984804e-01 -9.80041444e-01" +
    " -3.81010652e-01 9.76203203e-01 -9.46395814e-01 4.34752017e-01" +
    " -8.20855081e-01 8.28899384e-01 -7.23142922e-01 1.21546224e-01" +
    " 7.77958110e-02 -1.41583979e-01 -6.85890496e-01 -2.75579065e-01" +
    " -4.60603148e-01 -5.19717395e-01 -3.52825254e-01 -9.09923494e-01" +
    " 8.77114981e-02 6.29863918e-01 9.49251950e-01 7.80966103e-01" +
    " 9.11184490e-01 -3.02410156e-01 3.37515384e-01 -9.84004676e-01" +
    " 3.05141956e-01 -1.65781245e-01 5.95755339e-01 -9.93746400e-01"; */

     String[] vectorArray1 = new String[512];
     String[] vectorArray2 = new String[512];

     vectorArray1 = str1.split("\\ ");
     vectorArray2 = str2.split("\\ ");

     double expResult = 0.0;
     double result = FaceRecognitionManager.compareTwoFaces(vectorArray1, vectorArray2);

     System.out.print("result="+result);

     fail("The test case is a prototype.");
     }

     @Ignore
     @Test
     public void seperateVideo() 
     {
        System.out.println("seperateVideo");

        List<String> inputDataList = new ArrayList<>();
        List<String> inputDataFileNameList = new ArrayList<>();

        List<String> filepathList = new ArrayList<>();

        String folder = "D:\\gaittest";

        FileUtil.getFolderAllFiles(new File(folder),filepathList);

        for(String filepath : filepathList)
        {        
            if ( !filepath.endsWith("txt") )
                continue;
            
            System.out.println(" filepath="+filepath);
            String str = FileUtil.readTextFileToString(filepath);

            str = str.substring(str.indexOf("[")+1, str.indexOf("]")).trim();
            str = str.replace("\n", "");
            
            while(str.contains("  ") )
                str = str.replace("  ", " ");
            
            inputDataList.add(str);
            inputDataFileNameList.add(filepath);
        }

        for(int i=0;i<inputDataList.size();i++) 
        {
            String[] vectorArray1 = new String[512];
            String[] vectorArray2 = new String[512];

            for(int j=0;j<inputDataList.size();j++)
            {
                if ( j== i )
                    continue;
                
                vectorArray1 = inputDataList.get(i).split("\\ ");
                vectorArray2 = inputDataList.get(j).split("\\ ");
 
                double result = FaceRecognitionManager.compareTwoFaces(vectorArray1, vectorArray2);

                System.out.println("score="+result+" i="+inputDataFileNameList.get(i)+" j="+inputDataFileNameList.get(j));
              
                if ( result < 5 )
                {
                    System.out.println("------ score="+result+" same people："+inputDataFileNameList.get(i)+" j="+inputDataFileNameList.get(j));
                }
            }
        }
    }

 
}
