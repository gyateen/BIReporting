package com.power2sme.etl.entity;


import lombok.Data;

@Data
public class ETLColumnHeader {

	String name;
	int type;
	int length;
}
