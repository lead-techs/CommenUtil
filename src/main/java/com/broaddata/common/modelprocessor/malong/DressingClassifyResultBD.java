/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.broaddata.common.modelprocessor.malong;

import cn.productai.api.pai.response.DressingClassifyResult;
import cn.productai.api.pai.response.DressColor;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Transfer DressingClassifyResult from MalongTech to Broaddata's definition
 * @author Xin Wang <xin.wang@broaddata.cn>
 */
public class DressingClassifyResultBD {
    private String item;
    private String itemEN;
    private double left;
    private double top;
    private double right;
    private double bottom;
    private String color1;
    private String colorEN1;
    private double colorPercent1;
    private String color2;
    private String colorEN2;
    private double colorPercent2;
    private String texture;
    private String textureEN;
    
    public DressingClassifyResultBD(String item,
            String itemEN,
            double left,
            double top,
            double right,
            double bottom,
            String color1,
            String colorEN1,
            double colorPercent1,
            String color2,
            String colorEN2,
            double colorPercent2,
            String texture,
            String textureEN) {
        this.item = item;
        this.itemEN = itemEN;
        this.top = top;
        this.left = left;
        this.right = right;
        this.bottom = bottom;
        this.color1 = color1;
        this.colorEN1 = colorEN1;
        this.colorPercent1 = colorPercent1;
        this.color2 = color2;
        this.colorEN2 = colorEN2;
        this.colorPercent2 = colorPercent2;
        this.texture = texture;
        this.textureEN = textureEN;
    }
    
    public String getItem() {
        return item;
    }
    
    public void setItem(String item) {
        this.item = item;
    }
    
    public String getItemEN() {
        return itemEN;
    }
    
    public void setItemEN(String itemEN) {
        this.itemEN = itemEN;
    }
    
    public double getLeft() {
        return left;
    }
    
    public void setLeft(double left) {
        this.left = left;
    }
    
    public double getTop() {
        return top;
    }
    
    public void setTop(double top) {
        this.top = top;
    }
    
    public double getRight() {
        return right;
    }
    
    public void setRight(double right) {
        this.right = right;
    }
    
    public double getBottom() {
        return bottom;
    }
    
    public void setBottom(double bottom) {
        this.bottom = bottom;
    }
    
    public String getColor1() {
        return color1;
    }
    
    public void setColor1(String color1) {
        this.color1 = color1;
    }
    
    public String getColorEN1() {
        return colorEN1;
    }
    
    public void setColorEN1(String colorEN1) {
        this.colorEN1 = colorEN1;
    }
    
    public double getColorPercent1() {
        return colorPercent1;
    }
    
    public void setColorPercent1(double colorPercent1) {
        this.colorPercent1 = colorPercent1;
    }
    
    public String getColor2() {
        return color2;
    }
    
    public void setColor2(String color2) {
        this.color2 = color2;
    }
    
    public String getColorEN2() {
        return colorEN2;
    }
    
    public void setColorEN2(String colorEN2) {
        this.colorEN2 = colorEN2;
    }
    
    public double getColorPercent2() {
        return colorPercent2;
    }
    
    public void setColorPercent2(double colorPercent2) {
        this.colorPercent2 = colorPercent2;
    }
    
    public String getTexture() {
        return texture;
    }
    
    public void setTexture(String texture) {
        this.texture = texture;
    }
    
    public String getTextureEN() {
        return textureEN;
    }
    
    public void setTextureEN(String textureEN) {
        this.textureEN = textureEN;
    }
    
    public static DressingClassifyResultBD fromDressingClassifyResult(DressingClassifyResult result) {
        String item = result.getItem();
        String itemEN = result.getItemEN();
        double left = result.getBox()[0];
        double top = result.getBox()[1];
        double right = left + result.getBox()[2];
        double bottom = top + result.getBox()[3];
        
        String[] textures = result.getTextures();
        String texture = textures.length > 0? textures[0]: "";
        String[] textureENs = result.getTexturesEN();
        String textureEN = textureENs.length > 0? textureENs[0]: "";
        
        DressColor colors[] = result.getColors();
        Arrays.sort(colors, Comparator.comparing((DressColor color) -> color.getPercent()).reversed());
        String color1 = colors[0].getCnName();
        String colorEN1 = colors[0].getEnName();
        double colorPercent1 = colors[0].getPercent();
        String color2 = colors.length > 1? colors[1].getCnName(): "";
        String colorEN2 = colors.length > 1? colors[1].getEnName(): "";
        double colorPercent2 = colors.length > 1? colors[1].getPercent(): 0.0;
        
        return new DressingClassifyResultBD(item, itemEN, left, top, right, bottom,
                color1, colorEN1, colorPercent1, color2, colorEN2, colorPercent2,
                texture, textureEN);
    }
}
